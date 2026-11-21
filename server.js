// MeetingMet - server penghubung (signaling) sederhana. Tidak menyimpan data apa pun.
const http = require('http');
const { WebSocketServer } = require('ws');
const ORIGIN = process.env.ORIGIN || ''; // opsional: https://domain-anda.com
const rooms = new Map();
let n = 1;
const srv = http.createServer((q, s) => { s.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' }); s.end('MeetingMet server OK'); });
const wss = new WebSocketServer({ server: srv, maxPayload: 16384 });
function bp(r) { const o = JSON.stringify({ t: 'peers', list: [...r.values()].map(c => ({ id: c.id, p: c.p })) }); r.forEach(c => c.readyState === 1 && c.send(o)); }
wss.on('connection', (ws, req) => {
  if (ORIGIN && req.headers.origin !== ORIGIN) return ws.close(1008);
  const code = (new URL(req.url, 'http://x').searchParams.get('room') || 'rapat').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 30) || 'rapat';
  const r = rooms.get(code) || new Map(); rooms.set(code, r);
  if (r.size >= 20) return ws.close(1013);
  const id = 'p' + (n++).toString(36) + Math.random().toString(36).slice(2, 6);
  ws.id = id; ws.p = {}; ws.alive = true; r.set(id, ws);
  ws.send(JSON.stringify({ t: 'welcome', id })); bp(r);
  ws.on('pong', () => ws.alive = true);
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'pres' && m.p && typeof m.p === 'object' && JSON.stringify(m.p).length < 1000) { ws.p = m.p; bp(r); }
    else if (m.t === 'emit' && typeof m.topic === 'string') {
      const o = JSON.stringify({ t: 'emit', topic: m.topic.slice(0, 30), data: m.data, from: id });
      const to = m.data && m.data.to;
      if (to) { const c = r.get(to); if (c && c.readyState === 1) c.send(o); }
      else r.forEach(c => c.readyState === 1 && c.send(o));
    }
  });
  ws.on('close', () => { r.delete(id); if (!r.size) rooms.delete(code); else bp(r); });
});
setInterval(() => wss.clients.forEach(c => { if (!c.alive) return c.terminate(); c.alive = false; c.ping(); }), 30000);
srv.listen(process.env.PORT || 3000, () => console.log('MeetingMet server siap'));
