/**
 * 올타(OLTA) 실시간 멀티플레이어 서버
 * - Express: public/ 폴더(게임 화면)를 서빙
 * - Socket.io: 접속 / 이동 / 채팅 / 퇴장을 모든 접속자에게 실시간 중계
 *
 * 실행:  npm install  →  npm start
 * 포트:  process.env.PORT (없으면 3000)
 */
const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS) || 200;

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },          // 게임 화면을 다른 주소에서 열어도 접속 가능
  pingInterval: 20000,            // 20초마다 연결 확인 (무료 호스팅의 유휴 끊김 방지)
  pingTimeout: 25000,
  maxHttpBufferSize: 10 * 1024    // 한 번에 보낼 수 있는 데이터 크기 제한(10KB)
});

/* ---------- 정적 파일 + 상태 확인 ---------- */
app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (req, res) => res.json({ ok: true, online: players.size }));

/* ---------- 접속자 저장소 (메모리) ---------- */
const players = new Map(); // socket.id -> 접속자 정보

const ADJ = ['새벽', '달빛', '별빛', '은하', '보라', '몽글', '말랑', '반짝', '포근', '졸린'];
const NOUN = ['고양이', '토끼', '여우', '수집가', '여행자', '유니콘', '구름', '혜성', '올빼미', '라떼'];
const COLORS = ['#ff9ec4', '#b79cff', '#8fe3dc', '#ffd34e', '#ff9e7a', '#9fd0ff', '#c9a7ff', '#7ee8a0'];
const pick = a => a[Math.floor(Math.random() * a.length)];
const randomNick = () => pick(ADJ) + pick(NOUN) + '_' + String(Math.floor(Math.random() * 90) + 10);

/* ---------- 입력값 정리 (이상한 값이 들어와도 서버가 죽지 않게) ---------- */
const num = (v, lo, hi, d) => { v = Number(v); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, Math.round(v))) : d; };
const clean = (v, max) => String(v == null ? '' : v).replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, max);
const FACES = ['u', 'd', 'l', 'r'];

const publicInfo = p => ({
  id: p.id, nick: p.nick, color: p.color,
  x: p.x, y: p.y, floor: p.floor, scene: p.scene, face: p.face,
  moving: p.moving, item: p.item, outfit: p.outfit, sit: p.sit, hide: p.hide
});

/* 아주 단순한 속도 제한: 1초에 limit번까지만 처리 */
function limiter(limit, windowMs) {
  let start = Date.now(), n = 0;
  return () => { const now = Date.now(); if (now - start > windowMs) { start = now; n = 0; } return ++n <= limit; };
}

io.on('connection', socket => {
  if (players.size >= MAX_PLAYERS) { socket.emit('serverFull'); socket.disconnect(true); return; }

  /* 1) 접속: 랜덤 닉네임/색상 부여 → 현재 접속자 목록을 신규 유저에게 전송 */
  const me = {
    id: socket.id, nick: randomNick(), color: pick(COLORS),
    x: 900, y: 980, floor: 1, scene: 'plaza', face: 'd',
    moving: false, item: '', outfit: 0, sit: false, hide: false
  };
  players.set(socket.id, me);

  const others = [];
  for (const p of players.values()) if (p.id !== socket.id) others.push(publicInfo(p));
  socket.emit('welcome', { id: me.id, nick: me.nick, color: me.color, players: others });
  socket.emit('currentPlayers', others);                 // (호환용) 현재 접속자 목록
  socket.broadcast.emit('playerJoined', publicInfo(me));  // 기존 유저들에게 신규 유저 알림
  io.emit('onlineCount', players.size);

  const moveOK = limiter(40, 1000), chatOK = limiter(4, 3000), nickOK = limiter(3, 5000);

  /* 닉네임 변경 (클라이언트가 저장해 둔 닉네임을 올려 보냄) */
  socket.on('setNick', raw => {
    if (!nickOK()) return;
    const nick = clean(raw, 12);
    if (!nick) return;
    me.nick = nick;
    io.emit('playerRenamed', { id: me.id, nick });
  });

  /* 2) 위치 이동: [X, Y, 층수, 방향, 이동중 여부, 손에 든 아이템] → 다른 모든 접속자에게 */
  socket.on('playerMove', d => {
    if (!d || typeof d !== 'object' || !moveOK()) return;
    me.x = num(d.x, -200, 4000, me.x);
    me.y = num(d.y, -200, 4000, me.y);
    me.floor = num(d.floor, 1, 4, 1);
    me.scene = clean(d.scene, 12).replace(/[^a-z0-9_]/gi, '') || 'plaza';
    me.face = FACES.includes(d.face) ? d.face : 'd';
    me.moving = !!d.moving;
    me.item = clean(d.item, 12);
    me.outfit = num(d.outfit, 0, 30, 0);
    me.sit = !!d.sit;
    me.hide = !!d.hide;
    socket.broadcast.emit('playerMoved', publicInfo(me));
  });

  /* 3) 채팅: 말풍선 텍스트를 전체 유저에게 */
  socket.on('chatMessage', raw => {
    if (!chatOK()) return;
    const text = clean(raw && typeof raw === 'object' ? raw.text : raw, 40);
    if (!text) return;
    io.emit('newChat', { id: me.id, nick: me.nick, color: me.color, text, t: Date.now() });
  });

  /* 4) 퇴장: 다른 유저 화면에서 캐릭터 즉시 제거 */
  socket.on('disconnect', () => {
    players.delete(socket.id);
    io.emit('playerLeft', socket.id);
    io.emit('onlineCount', players.size);
  });
});

server.listen(PORT, () => console.log('🌙 올타 서버 실행 중 → 포트 ' + PORT));

/* 호스팅이 서버를 끌 때(재배포 등) 깔끔하게 종료 */
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { io.close(); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref(); });
