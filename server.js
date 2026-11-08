/**
 * 올타(OLTA) 실시간 멀티플레이어 서버 v2
 * - Express: public/ 폴더(게임 화면)를 서빙
 * - Socket.io: 접속 / 이동 / 채팅 / 퇴장 + 친구 / 친구 상점 방문 / 방명록
 *
 * 실행:  npm install  →  npm start
 * 포트:  process.env.PORT (없으면 3000)
 *
 * 저장되는 것: 친구 / 방명록 / 상점 정보 / 내 게임 데이터(클라우드 백업)
 *  - 기본: 서버 메모리 + olta-data.json 파일
 *    (Render 무료 플랜은 서버가 잠들거나 재배포되면 이 파일이 사라질 수 있어요)
 *  - 오래 보관하려면: Render 환경변수에 UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN 을 넣으면
 *    무료 Upstash Redis에 저장돼서 서버가 재시작돼도 남아요.
 */
const path = require('path');
const http = require('http');
const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS) || 200;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'olta-data.json');
const MAX_USERS = 5000, MAX_FRIENDS = 50, MAX_NOTES = 40, MAX_NOTES_PER_AUTHOR = 5, MAX_SAVE = 150 * 1024;
const UP_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/$/, ''), UP_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const useDB = !!(UP_URL && UP_TOKEN);

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' },
  pingInterval: 20000,
  pingTimeout: 25000,
  maxHttpBufferSize: 256 * 1024
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (req, res) => res.json({ ok: true, online: players.size, store: useDB ? 'upstash' : 'file' }));

/* ================= 저장소 ================= */
const players = new Map();   // socket.id -> 지금 접속 중인 캐릭터
const users = new Map();     // pid -> 저장되는 유저 정보(친구/신청/상점/방명록)
const saves = new Map();     // pid -> {t, data}  내 게임 데이터(클라우드 백업)

function newUser(pid, nick, color) {
  return { pid, nick, color, friends: [], requests: [], shop: null, notes: [], seen: Date.now() };
}
const fixUser = u => Object.assign(newUser(u.pid, u.nick || '손님', u.color || '#c9a7ff'), u, { friends: u.friends || [], requests: u.requests || [], notes: u.notes || [] });

async function dbCmd(args) {
  const r = await fetch(UP_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + UP_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(args) });
  const j = await r.json(); if (j.error) throw new Error(j.error); return j.result;
}
async function dbPipeline(cmds) {
  if (!cmds.length) return;
  const r = await fetch(UP_URL + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + UP_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(cmds) });
  const j = await r.json(); if (!Array.isArray(j)) throw new Error('pipeline 응답 오류');
  const bad = j.find(x => x && x.error); if (bad) throw new Error(bad.error);
}

async function loadData() {
  if (useDB) {
    try {
      const keys = (await dbCmd(['KEYS', 'olta:u:*'])) || [];
      for (let i = 0; i < keys.length; i += 50) {
        const vals = (await dbCmd(['MGET', ...keys.slice(i, i + 50)])) || [];
        for (const v of vals) { try { const u = JSON.parse(v); if (u && typeof u.pid === 'string') users.set(u.pid, fixUser(u)); } catch (e) { /* 깨진 항목은 건너뜀 */ } }
      }
      console.log('Upstash에서 불러옴:', users.size + '명');
    } catch (e) { console.error('Upstash 불러오기 실패(빈 상태로 시작):', e.message); }
    return;
  }
  try {
    const raw = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    for (const u of Object.values(raw.users || {})) if (u && typeof u.pid === 'string') users.set(u.pid, fixUser(u));
    for (const [pid, sv] of Object.entries(raw.saves || {})) if (validPid(pid) && sv && typeof sv.data === 'string') saves.set(pid, sv);
    console.log('파일에서 불러옴:', users.size + '명');
  } catch (e) { /* 파일이 없으면 새로 시작 */ }
}
const dirty = new Set(), dirtySaves = new Set(); let saveTimer = null;
function schedule() { if (!saveTimer) saveTimer = setTimeout(flush, useDB ? 8000 : 3000); }
function markDirty(pid) { if (pid) dirty.add(pid); schedule(); }
function markSave(pid) { if (pid) dirtySaves.add(pid); schedule(); }
async function flush() {
  saveTimer = null; if (!dirty.size && !dirtySaves.size) return;
  const du = [...dirty], ds = [...dirtySaves]; dirty.clear(); dirtySaves.clear();
  if (useDB) {
    const cmds = [];
    for (const pid of du) { const u = users.get(pid); if (u) cmds.push(['SET', 'olta:u:' + pid, JSON.stringify(u)]); }
    for (const pid of ds) { const sv = saves.get(pid); if (sv) cmds.push(['SET', 'olta:s:' + pid, JSON.stringify(sv)]); }
    try { await dbPipeline(cmds); for (const pid of ds) if (!isOnline(pid)) saves.delete(pid); }
    catch (e) { console.error('Upstash 저장 실패(다시 시도):', e.message); du.forEach(p => dirty.add(p)); ds.forEach(p => dirtySaves.add(p)); schedule(); }
    return;
  }
  try {
    const uo = {}; for (const [pid, u] of users) uo[pid] = u; const so = {}; for (const [pid, sv] of saves) so[pid] = sv;
    fs.writeFileSync(DATA_FILE + '.tmp', JSON.stringify({ users: uo, saves: so }));
    fs.renameSync(DATA_FILE + '.tmp', DATA_FILE);
  } catch (e) { /* 저장 실패해도 서버는 계속 동작 */ }
}
async function getSave(pid) {
  if (saves.has(pid)) return saves.get(pid);
  if (!useDB) return null;
  try { const v = await dbCmd(['GET', 'olta:s:' + pid]); if (!v) return null; const sv = JSON.parse(v); if (sv && typeof sv.data === 'string') return sv; } catch (e) { console.error('저장 데이터 읽기 실패:', e.message); }
  return null;
}
function pruneUsers() {
  if (users.size <= MAX_USERS) return;
  const online = new Set([...players.values()].map(p => p.pid));
  const old = [...users.values()].filter(u => !online.has(u.pid) && !u.friends.length && !u.notes.length && !saves.has(u.pid)).sort((a, b) => a.seen - b.seen);
  for (const u of old) { if (users.size <= MAX_USERS) break; users.delete(u.pid); }
}

/* ================= 도우미 ================= */
const ADJ = ['새벽', '달빛', '별빛', '은하', '보라', '몽글', '말랑', '반짝', '포근', '졸린'];
const NOUN = ['고양이', '토끼', '여우', '수집가', '여행자', '유니콘', '구름', '혜성', '올빼미', '라떼'];
const COLORS = ['#ff9ec4', '#b79cff', '#8fe3dc', '#ffd34e', '#ff9e7a', '#9fd0ff', '#c9a7ff', '#7ee8a0'];
const pick = a => a[Math.floor(Math.random() * a.length)];
const randomNick = () => pick(ADJ) + pick(NOUN) + '_' + String(Math.floor(Math.random() * 90) + 10);
const num = (v, lo, hi, d) => { v = Number(v); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, Math.round(v))) : d; };
const clean = (v, max) => String(v == null ? '' : v).replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, max);
const FACES = ['u', 'd', 'l', 'r'];
const pidOf = uid => crypto.createHash('sha256').update('olta:' + uid).digest('hex').slice(0, 12);
function validPid(p) { return typeof p === 'string' && /^[0-9a-f]{12}$/.test(p); }

function limiter(limit, windowMs) {
  let start = Date.now(), n = 0;
  return () => { const now = Date.now(); if (now - start > windowMs) { start = now; n = 0; } return ++n <= limit; };
}
function socketsOf(pid) { const out = []; for (const p of players.values()) if (p.pid === pid) out.push(p.id); return out; }
function isOnline(pid) { return socketsOf(pid).length > 0; }
const toPids = (pid, ev, data) => socketsOf(pid).forEach(id => io.to(id).emit(ev, data));
const friendInfo = pid => { const u = users.get(pid); return u ? { pid, nick: u.nick, online: isOnline(pid) } : null; };
const publicInfo = p => ({
  id: p.id, pid: p.pid, nick: p.nick, color: p.color,
  x: p.x, y: p.y, floor: p.floor, scene: p.scene, face: p.face,
  moving: p.moving, item: p.item, outfit: p.outfit, sit: p.sit, hide: p.hide,
  mnt: p.mnt, mfl: p.mfl, mh: p.mh, swim: p.swim, cap: p.cap, dance: p.dance, lie: p.lie, emo: p.emo
});
const noteView = n => ({ id: n.id, from: n.from, nick: n.nick, text: n.text, t: n.t });
const ack = (cb, data) => { if (typeof cb === 'function') cb(data); };

/* ================= 3층 러브스튜디오 (3:3 미팅 방송): 서버가 진행자 ================= */
const LSK = Number(process.env.LS_TIME_SCALE) > 0 ? Number(process.env.LS_TIME_SCALE) : 1;   // 테스트용: 서버 대기시간 배율
const lsMs = ms => Math.max(5, Math.round(ms * LSK));
const LS_NAMES = ['달토끼', '별사탕', '은하수', '코코넛', '민트초코', '라떼', '보라돌이', '반딧불'];
const LS_INTROS = ['야식 좋아하는 새벽형 인간이에요', '로파이 들으면서 멍때리기 장인', '고양이 집사! 우주 산책 좋아해요', '춤은 못 춰도 열정은 만점!', '커피 없으면 말 안 해요 ☕', '별 보는 거 좋아하는 낭만파', '웃음 많은 편! 잘 부탁해요', '취미는 잠자기, 특기도 잠자기'];
const LS_DEFAULT_INTRO = '잘 부탁드려요! 두근두근 💓';
// 브라우저의 방송 진행 시간과 맞춤: 소개 6명×4.4초, 댄스 약 18.6초, 선택 마감 4.8초
const LS_T = { lobby: 8000, fill: 6500, ask: 18000, lead: 1500, intro: 26400, dance: 18800, scoreGrace: 1200, pickAfter: 4600, reset: 14000 };
const ls = { phase: 'idle', seats: [null, null, null, null, null, null], roster: null, startAt: 0, lobbyEnd: 0, intros: [], scores: [0, 0, 0, 0, 0, 0], picks: [-1, -1, -1, -1, -1, -1], reported: [], timers: [], askedAll: false };
const lsAfter = (ms, fn) => ls.timers.push(setTimeout(fn, lsMs(ms)));
function lsReset() { ls.timers.forEach(clearTimeout); Object.assign(ls, { phase: 'idle', seats: [null, null, null, null, null, null], roster: null, startAt: 0, lobbyEnd: 0, intros: [], scores: [0, 0, 0, 0, 0, 0], picks: [-1, -1, -1, -1, -1, -1], reported: [], timers: [], askedAll: false }); }
const lsSeatOf = pid => ls.seats.findIndex(s => s && s.pid === pid);
const lsHumanCount = () => ls.seats.filter(Boolean).length;
const lsView = () => ls.seats.map(s => s ? { pid: s.pid, nick: s.nick } : null);
function lsLobbyCast() { io.emit('lsLobby', { seats: lsView(), endsAt: ls.lobbyEnd, now: Date.now(), cancel: ls.phase !== 'lobby' }); }
function lsBotCard(i, used) {
  let k = Math.floor(Math.random() * LS_NAMES.length), g = 0; while (used.has(k) && g++ < 20) k = (k + 1) % LS_NAMES.length; used.add(k);
  return { h: 0, name: LS_NAMES[k], intro: LS_INTROS[Math.floor(Math.random() * LS_INTROS.length)] };
}
function lsDoFill() {
  if (ls.phase !== 'lobby') return; if (!lsHumanCount()) return lsReset();
  ls.phase = 'fill'; const used = new Set(); ls.roster = ls.seats.map((s, i) => s ? { h: 1, pid: s.pid, name: s.nick, intro: '' } : lsBotCard(i, used));
  io.emit('lsFill', { roster: ls.roster, now: Date.now() });
  lsAfter(LS_T.fill, lsDoAsk);
}
function lsDoAsk() {
  if (ls.phase !== 'fill') return; ls.phase = 'ask'; ls.intros = ls.roster.map(r => r.h ? null : r.intro);
  io.emit('lsAsk', { now: Date.now(), deadline: Date.now() + lsMs(LS_T.ask) }); lsAfter(LS_T.ask, lsDoStart);
}
function lsAllIntroIn() { return ls.roster.every((r, i) => !r.h || typeof ls.intros[i] === 'string'); }
function lsDoStart() {
  if (ls.phase !== 'ask') return; ls.phase = 'running'; const now = Date.now();
  ls.roster.forEach((r, i) => { if (r.h) r.intro = (typeof ls.intros[i] === 'string' && ls.intros[i]) ? ls.intros[i] : LS_DEFAULT_INTRO; });
  ls.startAt = now + lsMs(LS_T.lead); io.emit('lsStart', { roster: ls.roster, startAt: ls.startAt, now });
  const danceEnd = LS_T.lead + LS_T.intro + LS_T.dance; ls.scoreDeadline = now + lsMs(danceEnd + 5000);
  lsAfter(danceEnd + LS_T.scoreGrace, lsTryScores);
}
// 화면이 느린 사람도 점수를 보낼 수 있게: 모두 보고했거나 마감(최대 5초 추가)이 되면 확정
function lsTryScores() {
  if (ls.phase !== 'running') return;
  const all = ls.roster.every((r, i) => !r.h || ls.reported[i] != null);
  if (all || Date.now() >= ls.scoreDeadline) return lsDoScores();
  ls.timers.push(setTimeout(lsTryScores, 150));
}
function lsDoScores() {
  if (ls.phase !== 'running') return;
  ls.scores = ls.roster.map((r, i) => r.h ? (ls.reported[i] != null ? ls.reported[i] : 0) : 4 + Math.floor(Math.random() * 8));
  io.emit('lsScores', { scores: ls.scores });
  lsAfter(LS_T.pickAfter, lsDoPicks); lsAfter(LS_T.pickAfter + LS_T.reset, lsDoEnd);   // 선택 마감·초기화는 점수 확정 시각 기준
}
function lsDoPicks() {
  if (ls.phase !== 'running') return; const humanIdx = new Set(); ls.roster.forEach((r, i) => { if (r.h) humanIdx.add(i); });
  const picks = ls.roster.map((r, i) => {
    const opp = i < 3 ? [3, 4, 5] : [0, 1, 2];
    if (r.h && opp.includes(ls.picks[i])) return ls.picks[i];
    if (r.h) return opp[Math.floor(Math.random() * 3)];
    const w = opp.map(j => 1 + ls.scores[j] * .3 + (humanIdx.has(j) ? .3 : 0)), sum = w.reduce((a, b) => a + b, 0); let x = Math.random() * sum;
    for (let k = 0; k < 3; k++) { if (x < w[k]) return opp[k]; x -= w[k]; } return opp[2];
  });
  ls.picks = picks; io.emit('lsPicks', { picks, scores: ls.scores });
}
function lsDoEnd() { io.emit('lsEnd', {}); lsReset(); }
// 참가자가 사라짐(그만두기 / 자리 이탈 / 접속 종료): 로비 단계면 자리만 비우고, 방송이 시작된 뒤면 외계인 친구가 자리를 이어받음
function lsLeave(pid) {
  const i = lsSeatOf(pid); if (i < 0) return;
  if (ls.phase === 'lobby') { ls.seats[i] = null; if (!lsHumanCount()) { lsReset(); } lsLobbyCast(); return; }
  if (ls.phase === 'idle') { ls.seats[i] = null; return; }
  const used = new Set(ls.roster.filter(r => !r.h).map(r => LS_NAMES.indexOf(r.name))); const card = lsBotCard(i, used); if (!card.intro) card.intro = LS_DEFAULT_INTRO;
  ls.seats[i] = null; ls.roster[i] = card; ls.intros[i] = card.intro;
  io.emit('lsQuitted', { idx: i, name: card.name, intro: card.intro });
  if (!lsHumanCount()) { io.emit('lsEnd', {}); return lsReset(); }
  if (ls.phase === 'ask' && lsAllIntroIn() && !ls.askedAll) { ls.askedAll = true; lsAfter(600, lsDoStart); }
}

/* ================= 오목 게임기: 주민(다른 접속자)과 대결 =================
 * 서버가 심판: 순서·빈칸·5목 판정·제한시간을 서버에서 확인하고 두 사람에게 같은 결과를 보내요. (보라 돌=선공, 흰 돌=후공)
 */
const OM_N = 15, OM_TURN = 45000, OM_DIRS = [[1, 0], [0, 1], [1, 1], [1, -1]];
const omQueue = [], omGames = new Map(), omByPid = new Map();
function omWin(b, x, y, who) {
  for (const [dx, dy] of OM_DIRS) {
    const line = [[x, y]];
    for (let s = 1; s < OM_N; s++) { const nx = x + dx * s, ny = y + dy * s; if (nx < 0 || ny < 0 || nx >= OM_N || ny >= OM_N || b[ny * OM_N + nx] !== who) break; line.push([nx, ny]); }
    for (let s = 1; s < OM_N; s++) { const nx = x - dx * s, ny = y - dy * s; if (nx < 0 || ny < 0 || nx >= OM_N || ny >= OM_N || b[ny * OM_N + nx] !== who) break; line.unshift([nx, ny]); }
    if (line.length >= 5) return line;
  }
  return null;
}
const omCast = (g, ev, data) => { for (const p of g.p) io.to(p.sid).emit(ev, data); };
function omTimer(g) { clearTimeout(g.timer); g.turnEnd = Date.now() + lsMs(OM_TURN); g.timer = setTimeout(() => omFinish(g, 1 - g.turn, 'timeout'), lsMs(OM_TURN)); }
function omFinish(g, winner, reason, line) {
  if (g.over) return; g.over = true; clearTimeout(g.timer);
  omCast(g, 'omokOver', { id: g.id, winner, reason, line: line || null });
  g.p.forEach(p => omByPid.delete(p.pid)); omGames.delete(g.id);
}
function omMatch(a, b) {   // a가 먼저 기다렸으니 보라(선공)
  const g = { id: crypto.randomBytes(5).toString('hex'), p: [a, b], b: new Array(OM_N * OM_N).fill(-1), turn: 0, n: 0, over: false, timer: null, turnEnd: 0 };
  omGames.set(g.id, g); omByPid.set(a.pid, g.id); omByPid.set(b.pid, g.id); omTimer(g);
  g.p.forEach((p, i) => io.to(p.sid).emit('omokStart', { id: g.id, you: i, opp: { nick: g.p[1 - i].nick }, turnEnd: g.turnEnd, now: Date.now() }));
}
function omLeave(pid) {
  const qi = omQueue.findIndex(q => q.pid === pid); if (qi >= 0) omQueue.splice(qi, 1);
  const gid = omByPid.get(pid); if (!gid) return; const g = omGames.get(gid); if (!g) return omByPid.delete(pid);
  omFinish(g, 1 - g.p.findIndex(p => p.pid === pid), 'left');
}

/* ================= 접속 ================= */
io.on('connection', socket => {
  if (players.size >= MAX_PLAYERS) { socket.emit('serverFull'); socket.disconnect(true); return; }

  // 기기별 고유 uid(비밀) → 공개용 pid. uid가 없으면 이번 접속만 쓰는 임시 ID
  let uid = socket.handshake.auth && socket.handshake.auth.uid;
  if (typeof uid !== 'string' || !/^[A-Za-z0-9_-]{16,64}$/.test(uid)) uid = crypto.randomBytes(16).toString('hex');
  const pid = pidOf(uid);

  let rec = users.get(pid);
  if (!rec) { rec = newUser(pid, randomNick(), pick(COLORS)); users.set(pid, rec); pruneUsers(); }
  rec.seen = Date.now();

  const me = {
    id: socket.id, pid, nick: rec.nick, color: rec.color,
    x: 900, y: 980, floor: 1, scene: 'plaza', face: 'd',
    moving: false, item: '', outfit: 0, sit: false, hide: false,
    mnt: -1, mfl: false, mh: 0, swim: false, cap: 0, dance: false, lie: false, emo: 0
  };
  players.set(socket.id, me);

  const others = [];
  for (const p of players.values()) if (p.id !== socket.id) others.push(publicInfo(p));
  socket.emit('welcome', {
    id: me.id, pid, nick: me.nick, color: me.color, players: others,
    friends: rec.friends.map(friendInfo).filter(Boolean),
    requests: rec.requests.map(friendInfo).filter(Boolean)
  });
  socket.emit('currentPlayers', others);
  socket.broadcast.emit('playerJoined', publicInfo(me));
  io.emit('onlineCount', players.size);
  rec.friends.forEach(f => toPids(f, 'friendStatus', { pid, nick: me.nick, online: true }));

  const moveOK = limiter(40, 1000), chatOK = limiter(4, 3000), nickOK = limiter(3, 5000);
  const frOK = limiter(12, 10000), gbPostOK = limiter(1, 8000), gbOK = limiter(20, 5000), shopOK = limiter(4, 3000), visitOK = limiter(6, 5000);

  socket.on('setNick', raw => {
    if (!nickOK()) return;
    const nick = clean(raw, 12); if (!nick) return;
    me.nick = nick; rec.nick = nick; markDirty(pid);
    io.emit('playerRenamed', { id: me.id, pid, nick });
    rec.friends.forEach(f => toPids(f, 'friendStatus', { pid, nick, online: true }));
  });

  socket.on('playerMove', d => {
    if (!d || typeof d !== 'object' || !moveOK()) return;
    me.x = num(d.x, -200, 4000, me.x);
    me.y = num(d.y, -200, 4000, me.y);
    me.floor = num(d.floor, 1, 4, 1);
    me.scene = clean(d.scene, 24).replace(/[^a-z0-9_]/gi, '') || 'plaza';
    me.face = FACES.includes(d.face) ? d.face : 'd';
    me.moving = !!d.moving;
    me.item = clean(d.item, 12);
    me.outfit = num(d.outfit, 0, 30, 0);
    me.sit = !!d.sit;
    me.hide = !!d.hide;
    me.mnt = num(d.mnt, -1, 9, -1); me.mfl = !!d.mfl; me.mh = num(d.mh, 0, 120, 0); me.swim = !!d.swim; me.cap = num(d.cap, 0, 9, 0);
    me.dance = !!d.dance; me.lie = !!d.lie; me.emo = num(d.emo, 0, 2, 0);
    socket.broadcast.emit('playerMoved', publicInfo(me));
  });

  socket.on('chatMessage', raw => {
    if (!chatOK()) return;
    const text = clean(raw && typeof raw === 'object' ? raw.text : raw, 40);
    if (!text) return;
    io.emit('newChat', { id: me.id, pid, nick: me.nick, color: me.color, text, t: Date.now() });
  });

  /* ---------- 친구 ---------- */
  socket.on('friendRequest', (d, cb) => {
    if (!frOK()) return ack(cb, { ok: false, msg: '잠시 후 다시 시도해 주세요' });
    const to = d && d.to; if (!validPid(to)) return ack(cb, { ok: false, msg: '상대를 찾을 수 없어요' });
    const target = users.get(to);
    if (!target) return ack(cb, { ok: false, msg: '상대를 찾을 수 없어요' });
    if (to === pid) return ack(cb, { ok: false, msg: '자기 자신에게는 신청할 수 없어요' });
    if (rec.friends.includes(to)) return ack(cb, { ok: false, msg: '이미 친구예요' });
    if (rec.friends.length >= MAX_FRIENDS || target.friends.length >= MAX_FRIENDS) return ack(cb, { ok: false, msg: '친구 수가 가득 찼어요' });
    if (rec.requests.includes(to)) { acceptFriend(pid, to); return ack(cb, { ok: true, msg: target.nick + '님과 친구가 되었어요!', accepted: true }); } // 서로 신청 → 자동 수락
    if (target.requests.includes(pid)) return ack(cb, { ok: false, msg: '이미 신청을 보냈어요' });
    if (target.requests.length >= 30) return ack(cb, { ok: false, msg: '상대가 받을 수 있는 신청이 가득 찼어요' });
    target.requests.push(pid); markDirty(to);
    toPids(to, 'friendRequested', { from: { pid, nick: me.nick } });
    ack(cb, { ok: true, msg: target.nick + '님에게 친구 신청을 보냈어요' });
  });

  socket.on('friendRespond', (d, cb) => {
    if (!frOK()) return ack(cb, { ok: false });
    const from = d && d.from; if (!validPid(from) || !rec.requests.includes(from)) return ack(cb, { ok: false, msg: '이미 처리된 신청이에요' });
    rec.requests = rec.requests.filter(p => p !== from);
    if (d.accept) { acceptFriend(pid, from); ack(cb, { ok: true, accepted: true }); }
    else { markDirty(pid); toPids(from, 'friendDeclined', { pid, nick: me.nick }); ack(cb, { ok: true, accepted: false }); }
  });

  socket.on('friendList', cb => ack(cb, { friends: rec.friends.map(friendInfo).filter(Boolean), requests: rec.requests.map(friendInfo).filter(Boolean) }));

  /* ---------- 내 상점 정보 · 친구 상점 방문 ---------- */
  socket.on('shopSnapshot', d => {
    if (!d || typeof d !== 'object' || !shopOK()) return;
    rec.shop = { opened: !!d.opened, theme: num(d.theme, 0, 2, 0), stock: [0, 1, 2].map(i => num(d.stock && d.stock[i], 0, 20, 0)), note: clean(d.note, 40) };
    markDirty(pid);
  });

  socket.on('visitShop', (d, cb) => {
    if (!visitOK()) return ack(cb, { ok: false, msg: '잠시 후 다시 시도해 주세요' });
    const to = d && d.pid; if (!validPid(to) || !rec.friends.includes(to)) return ack(cb, { ok: false, msg: '친구의 상점만 놀러갈 수 있어요' });
    const t = users.get(to); if (!t) return ack(cb, { ok: false, msg: '친구를 찾을 수 없어요' });
    if (!isOnline(to)) return ack(cb, { ok: false, msg: t.nick + '님이 지금 접속 중이 아니에요' });
    ack(cb, { ok: true, owner: { pid: to, nick: t.nick }, snap: t.shop || { opened: false, theme: 0, stock: [0, 0, 0], note: '' } });
  });

  /* ---------- 내 게임 데이터 클라우드 백업 ---------- */
  const saveUpOK = limiter(2, 20000), saveGetOK = limiter(5, 10000);
  socket.on('saveUpload', (d, cb) => {
    if (!saveUpOK()) return ack(cb, { ok: false, msg: '잠시 후 다시 시도해 주세요' });
    const data = d && d.data; if (typeof data !== 'string' || !data.length || data.length > MAX_SAVE) return ack(cb, { ok: false, msg: '저장 데이터가 너무 크거나 비어 있어요' });
    try { const o = JSON.parse(data); if (!o || typeof o !== 'object' || !o.d || typeof o.d !== 'object') throw new Error('x'); } catch (e) { return ack(cb, { ok: false, msg: '저장 데이터 형식이 올바르지 않아요' }); }
    const t = Date.now(); saves.set(pid, { t, data }); markSave(pid); ack(cb, { ok: true, t });
  });
  socket.on('saveFetch', async (d, cb) => {
    if (!saveGetOK()) return ack(cb, { ok: false, msg: '잠시 후 다시 시도해 주세요' });
    const code = String((d && d.code) || '').replace(/[^A-Za-z0-9_-]/g, '');
    if (!/^[A-Za-z0-9_-]{16,64}$/.test(code)) return ack(cb, { ok: false, msg: '저장 코드 형식이 올바르지 않아요' });
    const sv = await getSave(pidOf(code));
    if (!sv) return ack(cb, { ok: false, msg: '이 코드로 저장된 데이터를 찾을 수 없어요' });
    ack(cb, { ok: true, t: sv.t, data: sv.data });
  });

  /* ---------- 러브스튜디오 방송 ---------- */
  const lsOK = limiter(30, 5000);
  socket.on('lsSit', (d, cb) => {
    if (!lsOK()) return ack(cb, { ok: false, msg: '잠시 후 다시 시도해 주세요' });
    const idx = Number.isInteger(d && d.idx) ? d.idx : -1; if (idx < 0 || idx > 5) return ack(cb, { ok: false, msg: '참가자 자리가 아니에요' });
    if (ls.phase !== 'idle' && ls.phase !== 'lobby') return ack(cb, { ok: false, msg: '지금은 방송 중이에요. 끝난 뒤에 다시 앉아 주세요' });
    if (ls.seats[idx] && ls.seats[idx].pid !== pid) return ack(cb, { ok: false, msg: '이미 다른 참가자가 앉은 자리예요' });
    const old = lsSeatOf(pid); if (old >= 0 && old !== idx) ls.seats[old] = null;
    ls.seats[idx] = { pid, nick: me.nick, sid: socket.id };
    if (ls.phase === 'idle') { ls.phase = 'lobby'; ls.lobbyEnd = Date.now() + lsMs(LS_T.lobby); lsAfter(LS_T.lobby, lsDoFill); }
    if (lsHumanCount() === 6) { ls.timers.forEach(clearTimeout); ls.timers = []; ls.lobbyEnd = Date.now(); lsAfter(900, lsDoFill); }
    lsLobbyCast(); ack(cb, { ok: true, idx });
  });
  socket.on('lsLeave', () => { if (lsOK()) lsLeave(pid); });
  socket.on('lsQuit', () => { if (lsOK()) lsLeave(pid); });
  socket.on('lsIntro', d => {
    if (ls.phase !== 'ask') return; const i = lsSeatOf(pid); if (i < 0 || typeof ls.intros[i] === 'string') return;
    ls.intros[i] = clean(d && d.text, 20) || LS_DEFAULT_INTRO; ls.roster[i].intro = ls.intros[i];
    if (lsAllIntroIn() && !ls.askedAll) { ls.askedAll = true; lsAfter(600, lsDoStart); }
  });
  socket.on('lsScore', d => { if (ls.phase !== 'running') return; const i = lsSeatOf(pid); if (i < 0) return; ls.reported[i] = num(d && d.score, 0, 13, 0); });
  socket.on('lsPick', d => { if (ls.phase !== 'running') return; const i = lsSeatOf(pid); if (i < 0) return; const j = num(d && d.j, 0, 5, -1); const opp = i < 3 ? [3, 4, 5] : [0, 1, 2]; if (opp.includes(j)) ls.picks[i] = j; });

  /* ---------- 오목 ---------- */
  const omOK = limiter(30, 5000), omMoveOK = limiter(8, 1000);
  socket.on('omokQueue', (d, cb) => {
    if (!omOK()) return ack(cb, { ok: false, msg: '잠시 후 다시 시도해 주세요' });
    if (omByPid.has(pid)) return ack(cb, { ok: false, msg: '이미 대결 중이에요' });
    for (let i = omQueue.length - 1; i >= 0; i--) if (omQueue[i].pid === pid || !players.has(omQueue[i].sid)) omQueue.splice(i, 1);
    const other = omQueue.find(q => q.pid !== pid);
    if (other) { omQueue.splice(omQueue.indexOf(other), 1); omMatch(other, { pid, nick: me.nick, sid: socket.id }); return ack(cb, { ok: true, matched: true }); }
    omQueue.push({ pid, nick: me.nick, sid: socket.id }); ack(cb, { ok: true, waiting: true });
  });
  socket.on('omokLeave', () => { if (omOK()) omLeave(pid); });
  socket.on('omokMove', (d, cb) => {
    if (!omMoveOK()) return ack(cb, { ok: false, msg: '너무 빨라요' });
    const g = omGames.get(String(d && d.id)); if (!g || g.over) return ack(cb, { ok: false, msg: '진행 중인 대결이 없어요' });
    const i = g.p.findIndex(p => p.pid === pid); if (i < 0) return ack(cb, { ok: false, msg: '참가자가 아니에요' });
    if (g.turn !== i) return ack(cb, { ok: false, msg: '상대 차례예요' });
    const x = Number.isInteger(d.x) ? d.x : -1, y = Number.isInteger(d.y) ? d.y : -1;
    if (x < 0 || y < 0 || x >= OM_N || y >= OM_N) return ack(cb, { ok: false, msg: '판 밖이에요' });
    if (g.b[y * OM_N + x] >= 0) return ack(cb, { ok: false, msg: '이미 돌이 있어요' });
    g.b[y * OM_N + x] = i; g.n++; ack(cb, { ok: true });
    const line = omWin(g.b, x, y, i);
    if (line) { omCast(g, 'omokMoved', { id: g.id, x, y, by: i, turn: i, win: { winner: i, line }, now: Date.now() }); return omFinish(g, i, 'win', line); }
    if (g.n >= OM_N * OM_N) { omCast(g, 'omokMoved', { id: g.id, x, y, by: i, turn: -1, draw: true, now: Date.now() }); return omFinish(g, -1, 'draw'); }
    g.turn = 1 - i; omTimer(g); omCast(g, 'omokMoved', { id: g.id, x, y, by: i, turn: g.turn, turnEnd: g.turnEnd, now: Date.now() });
  });

  /* ---------- 방명록 ---------- */
  const canUseBoard = owner => validPid(owner) && users.has(owner) && (owner === pid || rec.friends.includes(owner));

  socket.on('gbOpen', (d, cb) => {
    if (!gbOK()) return ack(cb, { ok: false });
    const owner = d && d.owner; if (!canUseBoard(owner)) return ack(cb, { ok: false, msg: '친구의 방명록만 볼 수 있어요' });
    socket.join('gb:' + owner); const u = users.get(owner);
    ack(cb, { ok: true, owner: { pid: owner, nick: u.nick }, notes: u.notes.map(noteView) });
  });
  socket.on('gbClose', d => { if (d && validPid(d.owner)) socket.leave('gb:' + d.owner); });

  socket.on('gbPost', (d, cb) => {
    const owner = d && d.owner; if (!canUseBoard(owner)) return ack(cb, { ok: false, msg: '친구의 방명록에만 남길 수 있어요' });
    const text = clean(d.text, 30); if (!text) return ack(cb, { ok: false, msg: '한 글자 이상 적어 주세요' });
    if (!gbPostOK()) return ack(cb, { ok: false, msg: '조금 있다가 다시 남겨 주세요 (8초에 한 번)' });
    const u = users.get(owner);
    u.notes.push({ id: crypto.randomBytes(4).toString('hex'), from: pid, nick: me.nick, text, t: Date.now() });
    const mine = u.notes.filter(n => n.from === pid);
    if (mine.length > MAX_NOTES_PER_AUTHOR) u.notes = u.notes.filter(n => n !== mine[0]);
    if (u.notes.length > MAX_NOTES) u.notes.shift();
    markDirty(owner);
    io.to('gb:' + owner).emit('gbUpdate', { owner, notes: u.notes.map(noteView) });
    if (owner !== pid) toPids(owner, 'gbNew', { from: me.nick });
    ack(cb, { ok: true });
  });

  socket.on('gbDelete', (d, cb) => {
    if (!gbOK()) return ack(cb, { ok: false });
    const owner = d && d.owner; if (!canUseBoard(owner)) return ack(cb, { ok: false });
    const u = users.get(owner), n = u.notes.find(q => q.id === String(d.id || ''));
    if (!n || !(owner === pid || n.from === pid)) return ack(cb, { ok: false, msg: '삭제할 수 없어요' });
    u.notes = u.notes.filter(q => q !== n); markDirty(owner);
    io.to('gb:' + owner).emit('gbUpdate', { owner, notes: u.notes.map(noteView) });
    ack(cb, { ok: true });
  });

  socket.on('disconnect', () => {
    players.delete(socket.id);
    if (!isOnline(pid)) { lsLeave(pid); omLeave(pid); }
    rec.seen = Date.now();
    io.emit('playerLeft', socket.id);
    io.emit('onlineCount', players.size);
    if (!isOnline(pid)) rec.friends.forEach(f => toPids(f, 'friendStatus', { pid, nick: rec.nick, online: false }));
  });
});

function acceptFriend(a, b) {
  const ua = users.get(a), ub = users.get(b); if (!ua || !ub) return;
  ua.requests = ua.requests.filter(p => p !== b); ub.requests = ub.requests.filter(p => p !== a);
  if (!ua.friends.includes(b)) ua.friends.push(b);
  if (!ub.friends.includes(a)) ub.friends.push(a);
  markDirty(a); markDirty(b);
  toPids(a, 'friendAdded', friendInfo(b));
  toPids(b, 'friendAdded', friendInfo(a));
}

loadData().then(() => server.listen(PORT, () => console.log('🌙 올타 서버 실행 중 → 포트 ' + PORT + ' · 저장소: ' + (useDB ? 'Upstash Redis' : '서버 파일'))));

for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, async () => { setTimeout(() => process.exit(0), 3500).unref(); try { await flush(); } catch (e) {} io.close(); server.close(() => process.exit(0)); });
