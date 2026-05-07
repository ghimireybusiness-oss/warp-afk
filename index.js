/**
 * Minecraft Bot (mineflayer) + 24/7 Web Dashboard
 * ------------------------------------------------
 * - Connects to a Minecraft Java Edition server (1.20+) via mineflayer.
 * - Listens to in-game chat from a configured "owner" and runs commands
 *   (!follow, !stop, !come, !jump, !say <message>) using mineflayer-pathfinder.
 * - Anti-AFK: looks around / hops every few seconds.
 * - Anti-bot detection bypass: waits and moves naturally before performing actions.
 * - Death recovery: automatically uses /back command when bot dies.
 * - Auto-reconnect on disconnect/kick/error.
 * - Optional join sequence for AuthMe-style servers
 *   (/register on first join, /login afterwards, then /server <name>, /tpa <owner>, ...).
 * - Tiny Express + Socket.IO dashboard on PORT (default 3000) so an UptimeRobot
 *   monitor can ping the URL and keep a Replit project awake 24/7.
 *   The dashboard exposes Start / Stop / Reconnect controls and live status.
 *
 * All connection settings live in config.json — no need to edit this file.
 */

const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const { Server: SocketIOServer } = require('socket.io');
const mineflayer = require('mineflayer');
const { pathfinder, Movements, goals } = require('mineflayer-pathfinder');

// ---------------------------------------------------------------------------
// Load configuration
// ---------------------------------------------------------------------------
const configPath = path.join(__dirname, 'config.json');
if (!fs.existsSync(configPath)) {
  console.error('[FATAL] config.json not found next to index.js');
  process.exit(1);
}
const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));

const RECONNECT_DELAY = Number(config.reconnectDelayMs) || 5000;
const PREFIX = config.prefix || '!';
const OWNER = (config.owner || '').trim();
const ANTI_AFK = config.antiAfk || { enabled: true, minIntervalMs: 15000, maxIntervalMs: 30000 };
const ANTI_BOT = config.antiBot || { enabled: true, minDelayMs: 2000, maxDelayMs: 5000, minMoveDelayMs: 1000, maxMoveDelayMs: 3000 };
const JOIN_SEQ = config.joinSequence || { enabled: false };
const WEB_PORT = Number(process.env.PORT) || Number(config.webPort) || 3000;

// Tiny persistent state file to remember whether we have already registered
// the account on a server (so /register only runs once, /login on every join).
const statePath = path.join(__dirname, '.state.json');
function loadState() {
  try {
    if (fs.existsSync(statePath)) return JSON.parse(fs.readFileSync(statePath, 'utf8'));
  } catch (e) {
    log('Could not read .state.json, starting fresh:', e.message);
  }
  return {};
}
function saveState(state) {
  try {
    fs.writeFileSync(statePath, JSON.stringify(state, null, 2));
  } catch (e) {
    log('Could not write .state.json:', e.message);
  }
}

// Helper: timestamped log line.
function log(...args) {
  const stamp = new Date().toISOString().replace('T', ' ').replace('Z', '');
  console.log(`[${stamp}]`, ...args);
}
function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// ---------------------------------------------------------------------------
// Web dashboard (Express + Socket.IO)
// ---------------------------------------------------------------------------
const app = express();
const server = http.createServer(app);
const io = new SocketIOServer(server);

app.use(express.static(__dirname));
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'main.html')));
// Lightweight health endpoint that UptimeRobot can hit.
app.get('/health', (_req, res) => res.json({ ok: true, status: latestStatus }));

// Try the configured port first, then fall back to the next few in case a
// previous bot process is still holding it (very common during local dev).
function tryListen(port, attemptsLeft) {
  server.once('error', (err) => {
    if (err && err.code === 'EADDRINUSE' && attemptsLeft > 0) {
      log(`Port ${port} is in use, trying ${port + 1} ...`);
      tryListen(port + 1, attemptsLeft - 1);
    } else {
      log('Dashboard server error:', err && err.message ? err.message : err);
    }
  });
  server.listen(port, '0.0.0.0', () => {
    log(`Dashboard listening on http://0.0.0.0:${port}`);
  });
}
tryListen(WEB_PORT, 10);

// Last-known status, broadcast to any new connecting client.
let latestStatus = {
  state: 'offline',                // 'online' | 'warn' | 'offline'
  message: 'Bot not started yet.',
  server: `${config.host}:${config.port}`,
};

function broadcastStatus(state, message) {
  latestStatus = {
    state,
    message,
    server: `${config.host}:${config.port}`,
  };
  io.emit('bot_status', latestStatus);
  log(`[status] ${state}: ${message}`);
}

io.on('connection', (socket) => {
  socket.emit('bot_status', latestStatus);

  socket.on('control_bot', (command) => {
    switch (command) {
      case 'start':     botManager.start();     break;
      case 'stop':      botManager.stop();      break;
      case 'reconnect': botManager.reconnect(); break;
      default:          log('Unknown control command:', command);
    }
  });
});

// ---------------------------------------------------------------------------
// Bot manager — handles lifecycle so the web UI can start/stop/reconnect it.
// ---------------------------------------------------------------------------
let bot = null;
let antiAfkTimer = null;
let isFollowing = false;
let followTarget = null;
let intentionallyStopped = false;
let reconnectTimer = null;

const botManager = {
  start() {
    if (bot) {
      log('Start requested but bot is already running.');
      return;
    }
    intentionallyStopped = false;
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    bot = createBot();
  },

  stop() {
    if (!bot) {
      log('Stop requested but bot is not running.');
      broadcastStatus('offline', 'Bot stopped.');
      return;
    }
    intentionallyStopped = true;
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    try { bot.quit('stopped via dashboard'); } catch { /* noop */ }
    bot = null;
    cleanup();
    broadcastStatus('offline', 'Bot stopped.');
  },

  reconnect() {
    log('Manual reconnect requested.');
    broadcastStatus('warn', 'Reconnecting...');
    if (bot) {
      try { bot.quit('manual reconnect'); } catch { /* noop */ }
      bot = null;
      cleanup();
    }
    intentionallyStopped = false;
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      bot = createBot();
    }, 1000);
  },
};

// ---------------------------------------------------------------------------
// Anti-bot detection bypass: simulate natural player behavior
// ---------------------------------------------------------------------------
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function performActionWithAntiBot(bot, action) {
  if (!ANTI_BOT.enabled) {
    // If anti-bot is disabled, perform action immediately
    return action();
  }

  try {
    // Initial wait before performing the action
    const initialWait = randInt(ANTI_BOT.minDelayMs || 2000, ANTI_BOT.maxDelayMs || 5000);
    log(`[Anti-bot] Waiting ${initialWait}ms before action...`);
    await sleep(initialWait);

    // Perform a natural movement to appear like a real player
    if (bot && bot.entity) {
      const yaw = Math.random() * Math.PI * 2;
      const pitch = (Math.random() - 0.5) * (Math.PI / 4);
      bot.look(yaw, pitch, true).catch(() => {});

      // Small delay for movement
      const moveDelay = randInt(ANTI_BOT.minMoveDelayMs || 1000, ANTI_BOT.maxMoveDelayMs || 3000);
      await sleep(moveDelay);
    }

    // Now perform the actual action
    log('[Anti-bot] Performing action...');
    return action();
  } catch (err) {
    log('[Anti-bot] Error during action:', err && err.message ? err.message : err);
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Bot factory (creates a fresh bot every (re)connect)
// ---------------------------------------------------------------------------
function createBot() {
  log(`Connecting to ${config.host}:${config.port} as "${config.username}" ...`);
  broadcastStatus('warn', `Connecting to ${config.host}:${config.port} ...`);

  const newBot = mineflayer.createBot({
    host: config.host,
    port: Number(config.port) || 25565,
    username: config.username,
    auth: config.auth || 'offline', // "microsoft" for premium accounts
    version: config.version || false, // false => auto-detect; works for 1.20+
  });

  newBot.loadPlugin(pathfinder);

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  // AuthMe-style servers (e.g. Arctix) keep us in a "login limbo" — the
  // bot won't fully spawn until /register or /login succeeds, and they
  // kick after ~60s if we don't authenticate. So fire the auth commands
  // on `login`, NOT on `spawn`.
  newBot.on('login', () => {
    log('Logged in to server.');
    runAuthSequence(newBot)
      .catch((err) => log('Auth sequence error:', err && err.message ? err.message : err));
  });

  newBot.once('spawn', () => {
    log(`Bot spawned. Logged in as ${newBot.username}.`);
    broadcastStatus('online', `Connected as ${newBot.username}.`);

    const defaultMove = new Movements(newBot);
    defaultMove.canDig = false; // don't tear up the world while pathing
    newBot.pathfinder.setMovements(defaultMove);

    runAfterLoginCommands(newBot)
      .catch((err) => log('After-login command error:', err && err.message ? err.message : err))
      .finally(() => startAntiAfk(newBot));
  });

  newBot.on('health', () => {
    if (newBot.health <= 5) log(`Low health: ${newBot.health}/20`);
  });

  // -------------------------------------------------------------------------
  // Death handling: use /back command
  // -------------------------------------------------------------------------
  newBot.on('death', () => {
    log('Bot died! Attempting to use /back command...');
    performActionWithAntiBot(newBot, async () => {
      newBot.chat('/back');
    }).catch((err) => log('Failed to execute /back command:', err && err.message ? err.message : err));
  });

  // -------------------------------------------------------------------------
  // Chat / commands
  // -------------------------------------------------------------------------
  newBot.on('chat', (username, message) => {
    if (username === newBot.username) return;
    if (OWNER && username !== OWNER) return;
    if (!message.startsWith(PREFIX)) return;

    const rest = message.slice(PREFIX.length).trim();
    if (!rest) return;
    const [cmd, ...args] = rest.split(/\s+/);
    handleCommand(newBot, username, cmd.toLowerCase(), args);
  });

  // -------------------------------------------------------------------------
  // Pathfinder feedback
  // -------------------------------------------------------------------------
  newBot.on('goal_reached', () => log('Goal reached.'));
  newBot.on('path_update', (r) => {
    if (r.status === 'noPath') {
      log('No path to target — giving up.');
      try { newBot.chat("I can't reach you from here."); } catch {}
      stopFollowing(newBot);
    }
  });

  // -------------------------------------------------------------------------
  // Disconnect / error → auto-reconnect (unless user clicked Stop)
  // -------------------------------------------------------------------------
  newBot.on('kicked', (reason) => {
    const r = typeof reason === 'string' ? reason : JSON.stringify(reason);
    log('Kicked from server:', r);
    broadcastStatus('warn', `Kicked: ${r}`);
  });

  newBot.on('error', (err) => {
    const msg = err && err.message ? err.message : String(err);
    log('Bot error:', msg);
    broadcastStatus('warn', `Error: ${msg}`);
  });

  newBot.on('end', (reason) => {
    log(`Disconnected (${reason || 'unknown'}).`);
    cleanup();
    bot = null;

    if (intentionallyStopped) {
      broadcastStatus('offline', 'Bot stopped.');
      return;
    }

    broadcastStatus('warn', `Disconnected. Reconnecting in ${RECONNECT_DELAY}ms...`);
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (!bot && !intentionallyStopped) bot = createBot();
    }, RECONNECT_DELAY);
  });

  return newBot;
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------
function handleCommand(bot, username, cmd, args) {
  log(`Command from ${username}: ${cmd} ${args.join(' ')}`);

  switch (cmd) {
    case 'follow': {
      performActionWithAntiBot(bot, async () => {
        const target = bot.players[username] && bot.players[username].entity;
        if (!target) { bot.chat("I can't see you — get closer so I can find you."); return; }
        isFollowing = true;
        followTarget = username;
        bot.pathfinder.setGoal(new goals.GoalFollow(target, 1), true);
        bot.chat(`Following ${username}.`);
      }).catch((err) => log('Follow command error:', err && err.message ? err.message : err));
      break;
    }
    case 'stop': {
      performActionWithAntiBot(bot, async () => {
        stopFollowing(bot);
        bot.chat('Stopped.');
      }).catch((err) => log('Stop command error:', err && err.message ? err.message : err));
      break;
    }
    case 'come': {
      performActionWithAntiBot(bot, async () => {
        const target = bot.players[username] && bot.players[username].entity;
        if (!target) { bot.chat("I can't see you yet."); return; }
        const { x, y, z } = target.position;
        bot.pathfinder.setGoal(new goals.GoalNear(x, y, z, 1));
        bot.chat(`On my way to ${username}.`);
      }).catch((err) => log('Come command error:', err && err.message ? err.message : err));
      break;
    }
    case 'jump': {
      performActionWithAntiBot(bot, async () => {
        bot.setControlState('jump', true);
        setTimeout(() => bot.setControlState('jump', false), 250);
      }).catch((err) => log('Jump command error:', err && err.message ? err.message : err));
      break;
    }
    case 'say': {
      performActionWithAntiBot(bot, async () => {
        const message = args.join(' ').trim();
        if (!message) { bot.chat('Usage: !say <message>'); return; }
        bot.chat(message);
      }).catch((err) => log('Say command error:', err && err.message ? err.message : err));
      break;
    }
    default:
      bot.chat(`Unknown command: ${cmd}. Try: follow, stop, come, jump, say.`);
  }
}

function stopFollowing(bot) {
  if (bot && bot.pathfinder) bot.pathfinder.setGoal(null);
  isFollowing = false;
  followTarget = null;
}

// ---------------------------------------------------------------------------
// Join sequence: /register on first join, /login afterwards, then any
// configured "afterLoginCommands" (e.g. /server survival, /tpa <owner>).
// ---------------------------------------------------------------------------
function fillTemplate(str, vars) {
  return str.replace(/\{(\w+)\}/g, (_, k) => (k in vars ? vars[k] : `{${k}}`));
}

// Auth phase — runs as soon as the bot logs in (BEFORE spawn) so AuthMe
// plugins let us through their 60-second register/login timeout.
async function runAuthSequence(bot) {
  if (!JOIN_SEQ.enabled) return;

  const initialDelay = Number(JOIN_SEQ.initialDelayMs) || 2000;
  const password = JOIN_SEQ.password || '';
  const vars = { password, username: bot.username };

  await sleep(initialDelay);

  const state = loadState();
  const hostKey = `${config.host}:${config.port}`.toLowerCase();
  state.registeredHosts = state.registeredHosts || {};

  if (!state.registeredHosts[hostKey] && JOIN_SEQ.registerCommand) {
    const cmd = fillTemplate(JOIN_SEQ.registerCommand, vars);
    log(`First join on ${hostKey} — registering: ${cmd}`);
    bot.chat(cmd);
    state.registeredHosts[hostKey] = true;
    saveState(state);
  } else if (JOIN_SEQ.loginCommand) {
    const cmd = fillTemplate(JOIN_SEQ.loginCommand, vars);
    log(`Logging in: ${cmd}`);
    bot.chat(cmd);
  }
}

// Post-spawn phase — runs once we're actually in the world (so /server,
// /tpa etc. are valid and don't get swallowed by AuthMe).
async function runAfterLoginCommands(bot) {
  if (!JOIN_SEQ.enabled) return;

  const stepDelay = Number(JOIN_SEQ.stepDelayMs) || 2500;
  const password = JOIN_SEQ.password || '';
  const vars = { password, username: bot.username };

  // Small delay so the spawn is fully settled.
  await sleep(stepDelay);

  const afterCmds = Array.isArray(JOIN_SEQ.afterLoginCommands) ? JOIN_SEQ.afterLoginCommands : [];
  for (const raw of afterCmds) {
    if (!raw) continue;
    const cmd = fillTemplate(raw, vars);
    log(`Sending: ${cmd}`);
    bot.chat(cmd);
    await sleep(stepDelay);
  }
}

// ---------------------------------------------------------------------------
// Anti-AFK
// ---------------------------------------------------------------------------
function startAntiAfk(bot) {
  if (!ANTI_AFK.enabled) return;
  const minMs = Number(ANTI_AFK.minIntervalMs) || 15000;
  const maxMs = Number(ANTI_AFK.maxIntervalMs) || 30000;

  function tick() {
    if (!bot || !bot.entity) return;
    if (!isFollowing && (!bot.pathfinder || !bot.pathfinder.isMoving())) {
      if (Math.random() < 0.6) {
        const yaw = Math.random() * Math.PI * 2;
        const pitch = (Math.random() - 0.5) * (Math.PI / 4);
        bot.look(yaw, pitch, true).catch(() => {});
      } else {
        bot.setControlState('jump', true);
        setTimeout(() => bot.setControlState('jump', false), 250);
      }
    }
    antiAfkTimer = setTimeout(tick, randInt(minMs, maxMs));
  }
  antiAfkTimer = setTimeout(tick, randInt(minMs, maxMs));
}

function cleanup() {
  if (antiAfkTimer) { clearTimeout(antiAfkTimer); antiAfkTimer = null; }
  isFollowing = false;
  followTarget = null;
}

// ---------------------------------------------------------------------------
// Process safety nets
// ---------------------------------------------------------------------------
process.on('uncaughtException', (err) => {
  log('uncaughtException:', err && err.stack ? err.stack : err);
});
process.on('unhandledRejection', (reason) => {
  log('unhandledRejection:', reason);
});

// Auto-start the bot when the process boots so it works even without
// anyone opening the dashboard. The dashboard can still stop/restart it.
botManager.start();
