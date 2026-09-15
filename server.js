/**
 * Pawn vs Pawn — Multiplayer Server
 * ----------------------------------
 * A minimal, no-database WebSocket relay for real 1v1 online chess.
 *
 * Design goals (per product spec):
 *  - No sign up. No accounts. A player just picks a name + flag and plays.
 *  - Nothing is ever saved. No game history, no move logs. Everything lives
 *    only in process memory for the lifetime of a room, and is discarded
 *    the instant the room ends or a socket disconnects.
 *  - Color assignment is entirely up to the server (a coin flip at match
 *    time) — whatever side either player picked earlier is irrelevant.
 *  - Time control is matched on a best-effort basis: the server first
 *    tries to pair two players who picked the same time control. If
 *    nobody else is waiting with that same preference after a short
 *    grace period, it pairs with whoever else is waiting, at ANY time
 *    control, rather than make anyone wait indefinitely. Whichever
 *    player didn't get their original pick is told so ("sorry, that
 *    time control wasn't available — playing X instead").
 *  - Moves are server-confirmed: the server keeps its own authoritative
 *    chess.js board per room and only relays a move to the opponent
 *    after verifying it's legal and it was actually that player's turn.
 *    Nobody can send an illegal or out-of-turn move.
 *  - If either player disconnects (closes the tab, loses connection,
 *    navigates away) the game simply ends for the other player,
 *    immediately.
 *  - Nothing here is ever written to a database or a file.
 *
 * Run:
 *    npm install
 *    npm start
 *
 * Then open http://localhost:3000 (this server also serves the game's
 * client file directly if PawnVsPawn.html sits next to this file) — or
 * point a separately hosted copy of PawnVsPawn.html at this server's
 * URL with ?server=https://your-server-url.
 */

const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const { Chess } = require('chess.js');

const PORT = process.env.PORT || 3000;

// Time controls the client offers (seconds). Anything else sent by a
// client is clamped to the nearest/default value.
const ALLOWED_TIMES = [180, 300, 600];
const DEFAULT_TIME = 300;

// How long a player waits for someone with the SAME time control before
// the server widens the search to "anyone, any time control."
const FALLBACK_WAIT_MS = 4000;

const app = express();
app.use(cors());
app.use(express.static(__dirname)); // serves PawnVsPawn.html at "/" if present

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    waiting: waitingQueue.length,
    activeRooms: rooms.size
  });
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

/* ------------------------------------------------------------------ *
 *  In-memory-only state. Nothing here ever touches a disk or a DB.
 *  A restart of this process wipes every queue and every room.
 * ------------------------------------------------------------------ */

// { id, socket, profile, preferredTime, fallbackTimer }
let waitingQueue = [];

// roomId -> { chess, white: socketId, black: socketId, timeControlSec, drawOfferBy }
const rooms = new Map();

function makeRoomId() {
  return Math.random().toString(36).slice(2, 10);
}

function normalizeTimeControl(raw) {
  const n = parseInt(raw, 10);
  return ALLOWED_TIMES.includes(n) ? n : DEFAULT_TIME;
}

function sanitizeProfile(raw) {
  const name =
    raw && typeof raw.name === 'string' && raw.name.trim()
      ? raw.name.trim().slice(0, 24)
      : 'Player';
  // Flags are either a short emoji string or a data: URL the player uploaded
  // for this session only. Cap the size so nobody can smuggle a huge
  // payload through the relay.
  let flag = '🏳️';
  if (raw && typeof raw.flag === 'string' && raw.flag.length <= 300000) {
    flag = raw.flag;
  }
  const country =
    raw && typeof raw.country === 'string' ? raw.country.slice(0, 40) : '';
  return { name, flag, country };
}

function gameOverReason(chess) {
  if (chess.in_checkmate()) return 'checkmate';
  if (chess.in_stalemate()) return 'stalemate';
  if (chess.in_threefold_repetition()) return 'repetition';
  if (chess.insufficient_material()) return 'insufficient';
  if (chess.in_draw()) return 'draw';
  return 'over';
}

function removeFromQueue(socketId) {
  const idx = waitingQueue.findIndex((w) => w.id === socketId);
  if (idx === -1) return null;
  const entry = waitingQueue[idx];
  if (entry.fallbackTimer) clearTimeout(entry.fallbackTimer);
  waitingQueue.splice(idx, 1);
  return entry;
}

/**
 * Pair two sockets into a room. `finalTime` is whatever time control the
 * match actually uses. `fallbackFor` is a Set of socket ids who did NOT
 * get their originally preferred time control, so the client can show
 * the "sorry, that time control wasn't available" message to just them.
 */
function createRoom(entryA, entryB, finalTime, fallbackFor) {
  const roomId = makeRoomId();
  const chess = new Chess();

  // The only place color is decided. A pure coin flip — nothing about
  // what either player picked on their own screen matters here.
  const aIsWhite = Math.random() < 0.5;
  const whiteEntry = aIsWhite ? entryA : entryB;
  const blackEntry = aIsWhite ? entryB : entryA;
  const whiteSocket = whiteEntry.socket;
  const blackSocket = blackEntry.socket;

  rooms.set(roomId, {
    chess,
    white: whiteSocket.id,
    black: blackSocket.id,
    timeControlSec: finalTime,
    drawOfferBy: null
  });

  whiteSocket.data.roomId = roomId;
  whiteSocket.data.color = 'w';
  blackSocket.data.roomId = roomId;
  blackSocket.data.color = 'b';
  whiteSocket.join(roomId);
  blackSocket.join(roomId);

  whiteSocket.emit('matchFound', {
    roomId,
    color: 'w',
    timeControlSec: finalTime,
    opponent: blackSocket.data.profile,
    timeFallback: fallbackFor.has(whiteSocket.id) ? finalTime : null
  });
  blackSocket.emit('matchFound', {
    roomId,
    color: 'b',
    timeControlSec: finalTime,
    opponent: whiteSocket.data.profile,
    timeFallback: fallbackFor.has(blackSocket.id) ? finalTime : null
  });
}

function cleanupRoom(socket, notifyOpponent) {
  const roomId = socket.data.roomId;
  if (!roomId) return;
  if (notifyOpponent) {
    // Per spec: if either player leaves, the game simply ends for the other.
    socket.to(roomId).emit('opponentLeft');
  }
  rooms.delete(roomId);
  socket.data.roomId = null;
  socket.data.color = null;
}

io.on('connection', (socket) => {
  socket.data.roomId = null;
  socket.data.color = null;
  socket.data.profile = null;

  /* ---------------- Matchmaking ---------------- */
  socket.on('findMatch', (payload) => {
    if (socket.data.roomId) return; // already in a game
    if (waitingQueue.some((w) => w.id === socket.id)) return; // already queued

    const profile = sanitizeProfile(payload);
    const preferredTime = normalizeTimeControl(payload && payload.timeControl);
    socket.data.profile = profile;

    // 1. Try an exact time-control match first — the honest, preferred case.
    const exactIdx = waitingQueue.findIndex((w) => w.preferredTime === preferredTime);
    if (exactIdx !== -1) {
      const other = waitingQueue.splice(exactIdx, 1)[0];
      if (other.fallbackTimer) clearTimeout(other.fallbackTimer);
      createRoom(
        { socket, profile, preferredTime },
        other,
        preferredTime,
        new Set() // both got exactly what they asked for
      );
      return;
    }

    // 2. Nobody else wants the same time control right now. Wait briefly,
    //    then pair with ANYONE waiting, regardless of their time control,
    //    rather than leave a player hanging indefinitely.
    socket.emit('waiting', { preferredTime });
    const entry = { id: socket.id, socket, profile, preferredTime, fallbackTimer: null };
    entry.fallbackTimer = setTimeout(() => {
      // Make sure we're still actually waiting (not already matched/left).
      if (!waitingQueue.some((w) => w.id === socket.id)) return;
      removeFromQueue(socket.id);

      if (waitingQueue.length === 0) {
        // Still nobody at all — go back to waiting for a real opponent.
        const requeued = { id: socket.id, socket, profile, preferredTime, fallbackTimer: null };
        waitingQueue.push(requeued);
        return;
      }

      // Oldest waiting player, any time control — "connect anyone with anyone."
      const other = waitingQueue.shift();
      if (other.fallbackTimer) clearTimeout(other.fallbackTimer);

      // Use whichever preference belongs to whoever has been waiting
      // longest — they were patient first, so their pick wins.
      const finalTime = other.preferredTime;
      const fallbackFor = new Set();
      if (preferredTime !== finalTime) fallbackFor.add(socket.id);
      if (other.preferredTime !== finalTime) fallbackFor.add(other.socket.id);

      createRoom({ socket, profile, preferredTime }, other, finalTime, fallbackFor);
    }, FALLBACK_WAIT_MS);
    waitingQueue.push(entry);
  });

  socket.on('cancelFindMatch', () => {
    removeFromQueue(socket.id);
  });

  /* ---------------- Gameplay ---------------- */
  socket.on('move', (payload) => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);
    if (!room || !payload) return;

    const myColor = socket.data.color;
    if (room.chess.turn() !== myColor) return; // not your turn — reject

    let move;
    try {
      move = room.chess.move({
        from: payload.from,
        to: payload.to,
        promotion: payload.promotion || undefined
      });
    } catch (e) {
      move = null;
    }
    if (!move) return; // illegal move — silently reject, board stays as-is

    room.drawOfferBy = null; // any move clears a pending draw offer
    io.to(roomId).emit('moveMade', {
      from: move.from,
      to: move.to,
      promotion: move.promotion || null,
      san: move.san,
      color: myColor
    });

    if (room.chess.game_over()) {
      io.to(roomId).emit('gameOver', { reason: gameOverReason(room.chess) });
    }
  });

  socket.on('resign', () => {
    const roomId = socket.data.roomId;
    if (!roomId || !rooms.has(roomId)) return;
    socket.to(roomId).emit('opponentResigned');
  });

  socket.on('offerDraw', () => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);
    if (!room) return;
    room.drawOfferBy = socket.data.color;
    socket.to(roomId).emit('drawOffered');
  });

  socket.on('drawResponse', (payload) => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);
    if (!room) return;
    room.drawOfferBy = null;
    socket.to(roomId).emit('drawResponse', { accept: !!(payload && payload.accept) });
  });

  /* ---------------- Leaving ---------------- */
  socket.on('leaveRoom', () => cleanupRoom(socket, true));

  socket.on('disconnect', () => {
    removeFromQueue(socket.id);
    cleanupRoom(socket, true);
  });
});

server.listen(PORT, () => {
  console.log(`Pawn vs Pawn multiplayer server listening on port ${PORT}`);
});
