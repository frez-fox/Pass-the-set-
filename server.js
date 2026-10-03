'use strict';
/*
  Pass the Set - room server (no npm packages needed, Node 18+).
  - Serves public/index.html on every path.
  - WebSocket endpoint: any path with an Upgrade header.
  - The HOST's browser runs the game rules. This server only keeps rooms and relays messages,
    so it stays small and cheap to run.
*/
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
// Works with index.html inside a public/ folder OR next to server.js
const INDEX = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')]
  .find((p) => fs.existsSync(p)) || path.join(__dirname, 'public', 'index.html');
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_MSG = 64 * 1024;
const CODE_RE = /^[\x21-\x7E]{6}$/;

const rooms = new Map();   // code -> room
let nextId = 1;

/* ---------------- minimal WebSocket framing ---------------- */
class Conn {
  constructor(socket) {
    this.socket = socket;
    this.id = nextId++;
    this.buf = Buffer.alloc(0);
    this.room = null;
    this.isHost = false;
    this.alive = true;
    this.frag = null;
    socket.on('data', (d) => this.onData(d));
    socket.on('close', () => this.onClose());
    socket.on('error', () => this.onClose());
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > MAX_MSG * 2) return this.destroy();
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_MSG) return this.destroy();
      if (!masked) return this.destroy();
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4);
      const payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      if (op === 8) { this.sendRaw(0x8, Buffer.alloc(0)); return this.destroy(); }
      if (op === 9) { this.sendRaw(0xA, payload); continue; }
      if (op === 0xA) continue;
      if (op === 1 || op === 0) {
        this.frag = op === 1 ? [payload] : (this.frag ? [...this.frag, payload] : null);
        if (fin && this.frag) {
          const text = Buffer.concat(this.frag).toString('utf8');
          this.frag = null;
          let msg; try { msg = JSON.parse(text); } catch (e) { continue; }
          try { handle(this, msg); } catch (e) { /* ignore bad messages */ }
        }
      }
    }
  }
  sendRaw(op, payload) {
    if (!this.alive) return;
    const n = payload.length;
    let head;
    if (n < 126) head = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(n, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(n), 2); }
    try { this.socket.write(Buffer.concat([head, payload])); } catch (e) { this.destroy(); }
  }
  send(obj) { this.sendRaw(1, Buffer.from(JSON.stringify(obj))); }
  destroy() { try { this.socket.destroy(); } catch (e) {} this.onClose(); }
  onClose() {
    if (!this.alive) return;
    this.alive = false;
    leave(this);
  }
}

/* ---------------- rooms ---------------- */
const str = (v, n) => String(v == null ? '' : v).slice(0, n);

function leave(c) {
  const r = c.room;
  if (!r) return;
  c.room = null;
  if (c.isHost) {
    for (const m of r.members.values()) { m.send({ t: 'closed' }); m.room = null; }
    rooms.delete(r.code);
  } else {
    r.members.delete(c.id);
    if (r.host.alive) r.host.send({ t: 'peer', id: c.id, left: true });
  }
}

function handle(c, m) {
  switch (m.t) {
    case 'host': {
      if (c.room) return;
      const code = str(m.code, 6);
      if (!CODE_RE.test(code) || rooms.has(code)) return c.send({ t: 'error', msg: 'code-taken' });
      const room = {
        code, rid: crypto.randomBytes(4).toString('hex'), net: m.net === 'local' ? 'local' : 'online',
        host: c, members: new Map(), open: true,
        mode: clampInt(m.mode, 4, 8, 4), deck: str(m.deck, 10), hostName: str(m.name, 12) || 'Host', count: 1
      };
      rooms.set(code, room);
      c.room = room; c.isHost = true;
      c.send({ t: 'hosted', id: c.id, code });
      break;
    }
    case 'update': {
      const r = c.room; if (!r || !c.isHost) return;
      r.count = clampInt(m.count, 1, 8, r.count);
      break;
    }
    case 'start': {
      const r = c.room; if (!r || !c.isHost) return;
      r.open = false;
      break;
    }
    case 'list': {
      const net = m.net === 'local' ? 'local' : 'online';
      const out = [];
      for (const r of rooms.values()) {
        if (r.open && r.count < r.mode) {
          out.push({ rid: r.rid, hostName: r.hostName, mode: r.mode, deck: r.deck, count: r.count });
        }
      }
      c.send({ t: 'rooms', rooms: out.slice(0, 30) });
      break;
    }
    case 'join': {
      if (c.room) return;
      let r = null;
      if (m.code != null) r = rooms.get(str(m.code, 6));
      else if (m.rid != null) for (const x of rooms.values()) if (x.rid === m.rid) r = x;
      if (!r || !r.open) return c.send({ t: 'error', msg: 'not-found' });
      if (r.members.size + 1 >= r.mode) return c.send({ t: 'error', msg: 'full' });
      c.room = r; c.isHost = false; r.members.set(c.id, c);
      c.send({ t: 'joined', id: c.id });
      r.host.send({ t: 'peer', id: c.id, joined: true, name: str(m.name, 12) || 'Player', prof: m.prof });
      break;
    }
    case 'msg': {
      const r = c.room; if (!r) return;
      if (c.isHost) {
        const pkt = { t: 'msg', data: m.data };
        if (m.to == null) { for (const x of r.members.values()) x.send(pkt); }
        else { const x = r.members.get(m.to); if (x) x.send(pkt); }
      } else {
        r.host.send({ t: 'msg', from: c.id, data: m.data });
      }
      break;
    }
  }
}
function clampInt(v, a, b, d) { v = parseInt(v, 10); return Number.isFinite(v) ? Math.max(a, Math.min(b, v)) : d; }

/* ---------------- HTTP ---------------- */
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
  fs.readFile(INDEX, (err, data) => {
    if (err) { res.writeHead(500); return res.end('index.html missing'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});
server.on('upgrade', (req, socket) => {
  if (String(req.headers.upgrade || '').toLowerCase() !== 'websocket' || !req.headers['sec-websocket-key']) return socket.destroy();
  const accept = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  new Conn(socket);
});
// keep connections alive on hosts that close idle sockets
setInterval(() => { for (const r of rooms.values()) { r.host.sendRaw(0x9, Buffer.alloc(0)); for (const m of r.members.values()) m.sendRaw(0x9, Buffer.alloc(0)); } }, 25000);

server.listen(PORT, () => {
  const nets = require('os').networkInterfaces();
  console.log('Pass the Set server running.');
  console.log('  This computer:  http://localhost:' + PORT);
  for (const k of Object.keys(nets)) for (const n of nets[k]) if (n.family === 'IPv4' && !n.internal) console.log('  Same Wi-Fi:     http://' + n.address + ':' + PORT);
});
