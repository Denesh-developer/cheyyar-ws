/* Cheyyar Squad - WebSocket relay server (rooms of up to 8 players, 4v4).
   Run:  npm install && node server.js        (Render sets PORT automatically) */
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 8, MAX_TEAM = 4;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end('OK');
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
const rooms = new Map(); // code -> { m: Map(id -> {ws,n,t,j}), jc: number }

const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };

function members(room) {
  const o = {};
  room.m.forEach((v, id) => { o[id] = { n: v.n, t: v.t, j: v.j }; });
  return o;
}
function hostId(room) {
  let best = null;
  room.m.forEach((v, id) => { if (!best || v.j < best.j || (v.j === best.j && id < best.id)) best = { id, j: v.j }; });
  return best && best.id;
}
function broadcast(room, o, exceptId) {
  const s = JSON.stringify(o);
  room.m.forEach((v, id) => { if (id !== exceptId && v.ws.readyState === 1) v.ws.send(s); });
}
function pushMembers(room) { broadcast(room, { t: 'm', m: members(room) }); }

function removeMember(ws) {
  const room = ws.room, id = ws.pid;
  if (!room || !id) return;
  const cur = room.m.get(id);
  if (cur && cur.ws === ws) {
    room.m.delete(id);
    broadcast(room, { t: 'pr', id });
    if (room.m.size === 0) rooms.delete(room.code);
    else pushMembers(room);
  }
  ws.room = null;
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.tokens = 300; ws.lastRefill = Date.now();
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    // simple rate limit: ~150 messages/second, burst 300
    const now = Date.now();
    ws.tokens = Math.min(300, ws.tokens + (now - ws.lastRefill) * 0.15);
    ws.lastRefill = now;
    if (ws.tokens < 1) return;
    ws.tokens -= 1;

    let v;
    try { v = JSON.parse(raw); } catch (e) { return; }
    if (!v || typeof v.t !== 'string') return;

    if (v.t === 'ping') { send(ws, { t: 'pong', c: v.c }); return; }

    if (v.t === 'join') {
      const code = String(v.code || '');
      const id = String(v.id || '').slice(0, 40);
      if (!/^[0-9]{4}$/.test(code) || !id) { send(ws, { t: 'err', e: 'bad' }); return; }
      if (ws.room) removeMember(ws);
      let room = rooms.get(code);
      if (!room) { room = { code, m: new Map(), jc: 0 }; rooms.set(code, room); }
      const old = room.m.get(id);
      if (old) { old.ws.room = null; room.m.delete(id); try { old.ws.close(); } catch (e) {} }
      if (room.m.size >= MAX_PLAYERS) { send(ws, { t: 'err', e: 'full' }); return; }
      let a = 0, b = 0, first = null;
      room.m.forEach((m) => { if (m.t === 0) a++; else b++; if (!first || m.j < first.j) first = m; });
      let team = a < b ? 0 : b < a ? 1 : (first ? 1 - first.t : 0);
      if ((team === 0 ? a : b) >= MAX_TEAM) team = 1 - team;
      ws.pid = id; ws.room = room;
      room.m.set(id, { ws, n: String(v.n || 'Player').slice(0, 14).replace(/[<>&"]/g, ''), t: team, j: ++room.jc });
      send(ws, { t: 'joined', team, m: members(room) });
      pushMembers(room);
      return;
    }

    const room = ws.room, id = ws.pid;
    if (!room || !id) return;
    const me = room.m.get(id);
    if (!me) return;

    switch (v.t) {
      case 'team': {
        const nt = v.team === 1 ? 1 : 0;
        if (nt === me.t) return;
        let c = 0;
        room.m.forEach((m) => { if (m.t === nt) c++; });
        if (c >= MAX_TEAM) return;
        me.t = nt;
        pushMembers(room);
        break;
      }
      case 'start':
        if (hostId(room) === id) broadcast(room, { t: 'start' });
        break;
      case 'p':  broadcast(room, { t: 'p', id, d: v.d }, id); break;
      case 'pr': broadcast(room, { t: 'pr', id }, id); break;
      case 'b':  broadcast(room, { t: 'b', o: v.o }, id); break;
      case 'rs': broadcast(room, { t: 'rs', d: v.d }, id); break;
      case 'sh': broadcast(room, { t: 'sh', d: v.d }, id); break;
      case 'ev': {
        const target = room.m.get(String(v.to || ''));
        if (target) send(target.ws, { t: 'ev', d: v.d });
        break;
      }
      case 'leave': removeMember(ws); break;
    }
  });

  ws.on('close', () => removeMember(ws));
  ws.on('error', () => {});
});

// drop dead connections
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
}, 30000);

server.listen(PORT, () => console.log('cheyyar-ws listening on ' + PORT));
