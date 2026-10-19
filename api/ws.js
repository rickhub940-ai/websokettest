import { createServer } from 'node:http';
import express from 'express';
import { WebSocketServer } from 'ws';
import { randomUUID } from 'node:crypto';

const app = express();
app.use(express.json());

// ⚠️ in-memory state — บน Vercel จะหายเมื่ออินสแตนซ์ถูกแทนที่
// ถ้าต้องการความเสถียรจริง ให้เปลี่ยนไปใช้ Upstash Redis
const clients = new Map(); // id -> { ws, name, hwid, ip, connectedAt }

function broadcastClientList() {
  const list = [...clients.entries()].map(([id, c]) => ({
    id,
    name: c.name,
    hwid: c.hwid,
    ip: c.ip,
    connectedAt: c.connectedAt,
  }));
  const payload = JSON.stringify({ type: 'client_list', clients: list });
  for (const { ws } of clients.values()) {
    if (ws.readyState === ws.OPEN) ws.send(payload);
  }
}

// ---------- HTML UI ----------
const HTML = `<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Client Monitor</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Segoe UI', system-ui, sans-serif;
    background: #0b0f17; color: #e6edf3;
    min-height: 100vh; padding: 40px 24px;
  }
  .header {
    max-width: 1000px; margin: 0 auto 32px;
    display: flex; align-items: center;
    justify-content: space-between; flex-wrap: wrap; gap: 12px;
  }
  h1 {
    font-size: 26px; font-weight: 700;
    background: linear-gradient(135deg, #58a6ff, #a371f7);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }
  .status {
    display: flex; align-items: center; gap: 8px;
    font-size: 14px; color: #8b949e;
    background: #161b22; padding: 8px 16px;
    border-radius: 999px; border: 1px solid #21262d;
  }
  .dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: #3fb950; box-shadow: 0 0 8px #3fb950;
    animation: pulse 2s infinite;
  }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
  .grid {
    max-width: 1000px; margin: 0 auto;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 16px;
  }
  .card {
    background: #161b22; border: 1px solid #21262d;
    border-radius: 14px; padding: 20px;
    transition: 0.2s; animation: fadeIn 0.3s ease;
  }
  .card:hover {
    border-color: #58a6ff; transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(88, 166, 255, 0.12);
  }
  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(8px); }
    to { opacity: 1; transform: translateY(0); }
  }
  .card-top { display: flex; align-items: center; gap: 14px; margin-bottom: 16px; }
  .avatar {
    width: 48px; height: 48px; border-radius: 12px;
    background: linear-gradient(135deg, #58a6ff, #a371f7);
    display: flex; align-items: center; justify-content: center;
    font-size: 20px; font-weight: 700; color: #fff; flex-shrink: 0;
  }
  .name { font-size: 17px; font-weight: 600; color: #e6edf3; word-break: break-word; }
  .online {
    font-size: 12px; color: #3fb950;
    display: flex; align-items: center; gap: 5px; margin-top: 2px;
  }
  .online::before {
    content: ''; width: 6px; height: 6px;
    border-radius: 50%; background: #3fb950;
  }
  .row {
    display: flex; justify-content: space-between;
    padding: 8px 0; font-size: 13px; border-top: 1px solid #21262d;
  }
  .label { color: #8b949e; }
  .value {
    color: #c9d1d9; font-family: 'Consolas', monospace;
    text-align: right; word-break: break-all; max-width: 60%;
  }
  .actions {
    display: flex; flex-wrap: wrap; gap: 8px;
    margin-top: 14px; padding-top: 14px;
    border-top: 1px solid #21262d;
  }
  .btn {
    flex: 1 1 auto; min-width: 80px;
    padding: 8px 10px; font-size: 12px; font-weight: 600;
    border-radius: 8px; border: 1px solid #30363d;
    background: #21262d; color: #c9d1d9;
    cursor: pointer; transition: 0.15s;
    display: flex; align-items: center; justify-content: center; gap: 5px;
  }
  .btn:hover:not(:disabled) {
    background: #30363d; border-color: #58a6ff; color: #58a6ff;
  }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .btn.danger:hover:not(:disabled) {
    border-color: #f85149; color: #f85149; background: #2a1516;
  }
  .btn.success:hover:not(:disabled) {
    border-color: #3fb950; color: #3fb950; background: #12201a;
  }
  .btn.warn:hover:not(:disabled) {
    border-color: #d29922; color: #d29922; background: #2a2010;
  }
  .toast {
    position: fixed; bottom: 24px; right: 24px;
    background: #161b22; border: 1px solid #30363d;
    border-radius: 10px; padding: 12px 18px;
    font-size: 13px; color: #c9d1d9;
    box-shadow: 0 8px 24px rgba(0,0,0,0.4);
    opacity: 0; transform: translateY(10px);
    transition: 0.25s; pointer-events: none; z-index: 100;
  }
  .toast.show { opacity: 1; transform: translateY(0); }
  .toast.ok { border-color: #3fb950; }
  .toast.err { border-color: #f85149; }
  .empty {
    text-align: center; color: #8b949e;
    padding: 60px 20px; font-size: 15px;
  }
  .result-panel {
    margin-top: 8px; padding: 8px 10px;
    background: #0d1117; border-radius: 6px;
    font-family: 'Consolas', monospace; font-size: 11px;
    color: #7ee787; max-height: 80px; overflow-y: auto;
    word-break: break-all;
  }
</style>
</head>
<body>
  <div class="header">
    <h1>🎮 Client Monitor</h1>
    <div class="status">
      <span class="dot"></span>
      <span id="count">0 client</span>
    </div>
  </div>
  <div class="grid" id="grid"></div>
  <div class="toast" id="toast"></div>
  <script>
    const wsUrl = location.protocol === 'https:'
      ? 'wss://' + location.host + '/api/ws'
      : 'ws://' + location.host + '/api/ws';
    const ws = new WebSocket(wsUrl);
    const grid = document.getElementById('grid');
    const count = document.getElementById('count');
    const toast = document.getElementById('toast');
    let currentClients = [];
    const results = new Map();

    function shortHwid(hwid) {
      if (!hwid || hwid.length <= 12) return hwid;
      return hwid.slice(0, 6) + '...' + hwid.slice(-6);
    }

    function showToast(msg, type) {
      toast.textContent = msg;
      toast.className = 'toast show ' + (type || 'ok');
      clearTimeout(showToast._t);
      showToast._t = setTimeout(() => toast.classList.remove('show'), 2200);
    }

    async function sendCommand(clientId, command, args) {
      try {
        const res = await fetch('/api/command', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ clientId, command, args: args || [] }),
        });
        const data = await res.json();
        if (data.ok) showToast('ส่งคำสั่ง "' + command + '" สำเร็จ', 'ok');
        else showToast('ผิดพลาด: ' + data.error, 'err');
        return data;
      } catch (e) {
        showToast('เชื่อมต่อเซิร์ฟเวอร์ไม่ได้', 'err');
      }
    }

    function escapeHtml(s) {
      return String(s).replace(/[&<>"']/g, function(ch) {
        return { '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[ch];
      });
    }

    function render(clients) {
      currentClients = clients;
      count.textContent = clients.length + ' client';
      if (clients.length === 0) {
        grid.innerHTML = '<div class="empty">ยังไม่มี client เชื่อมต่ออยู่</div>';
        return;
      }
      grid.innerHTML = clients.map(function(c) {
        const initial = (c.name || '?')[0].toUpperCase();
        const since = new Date(c.connectedAt).toLocaleTimeString('th-TH');
        const lastResult = results.get(c.id);
        return '<div class="card" data-id="' + c.id + '">' +
          '<div class="card-top">' +
            '<div class="avatar">' + initial + '</div>' +
            '<div>' +
              '<div class="name">' + escapeHtml(c.name) + '</div>' +
              '<div class="online">ออนไลน์</div>' +
            '</div>' +
          '</div>' +
          '<div class="row"><span class="label">HWID</span>' +
            '<span class="value" title="' + escapeHtml(c.hwid) + '">' +
            escapeHtml(shortHwid(c.hwid)) + '</span></div>' +
          '<div class="row"><span class="label">IP</span>' +
            '<span class="value">' + escapeHtml(c.ip) + '</span></div>' +
          '<div class="row"><span class="label">เชื่อมต่อเมื่อ</span>' +
            '<span class="value">' + since + '</span></div>' +
          '<div class="actions">' +
            '<button class="btn success" data-cmd="ping">📡 Ping</button>' +
            '<button class="btn" data-cmd="message">💬 Message</button>' +
            '<button class="btn warn" data-cmd="rejoin">🔄 Rejoin</button>' +
            '<button class="btn danger" data-cmd="kick">⛔ Kick</button>' +
            '<button class="btn danger" data-cmd="ban">🚫 Ban</button>' +
          '</div>' +
          (lastResult ? '<div class="result-panel">' + escapeHtml(lastResult) + '</div>' : '') +
        '</div>';
      }).join('');

      grid.querySelectorAll('.card').forEach(function(card) {
        const id = card.dataset.id;
        card.querySelectorAll('.btn').forEach(function(btn) {
          btn.addEventListener('click', async function() {
            const cmd = btn.dataset.cmd;
            let args = [];
            if (cmd === 'message') {
              const text = prompt('พิมพ์ข้อความที่ต้องการส่ง:');
              if (!text) return;
              args = [text];
            } else if (cmd === 'kick') {
              if (!confirm('เตะ client นี้? (จะ reconnect เองตาม backoff)')) return;
            } else if (cmd === 'rejoin') {
              if (!confirm('บังคับให้ client reconnect ทันที?')) return;
            } else if (cmd === 'ban') {
              if (!confirm('แบน client นี้ถาวร? (จะไม่ reconnect อีก)')) return;
            }
            btn.disabled = true;
            await sendCommand(id, cmd, args);
            setTimeout(function() { btn.disabled = false; }, 600);
          });
        });
      });
    }

    ws.onmessage = function(event) {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'client_list') {
          render(msg.clients);
        } else if (msg.type === 'command_result') {
          results.set(msg.clientId, msg.id.slice(0, 6) + ' → ' + msg.result);
          render(currentClients);
        }
      } catch (e) {}
    };

    ws.onclose = function() {
      count.textContent = 'ขาดการเชื่อมต่อ';
    };
  </script>
</body>
</html>`;

// ---------- Routes ----------
app.get('/', (req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(HTML);
});

app.post('/api/command', (req, res) => {
  const { clientId, command, args } = req.body || {};
  if (!clientId || !command) {
    return res.status(400).json({ ok: false, error: 'missing params' });
  }
  const target = clients.get(clientId);
  if (!target || target.ws.readyState !== target.ws.OPEN) {
    return res.status(404).json({ ok: false, error: 'client not found' });
  }
  const commandId = randomUUID();
  target.ws.send(JSON.stringify({
    type: 'command',
    id: commandId,
    command,
    args: args || [],
  }));
  res.json({ ok: true, commandId });
});

// ---------- WebSocket ----------
const server = createServer(app);
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress;
  let clientId = null;

  ws.on('message', (raw) => {
    const text = raw.toString();
    if (text === 'ping') {
      ws.send('pong');
      return;
    }
    try {
      const msg = JSON.parse(text);

      if (msg.type === 'register') {
        clientId = randomUUID();
        clients.set(clientId, {
          ws,
          name: msg.name || 'Unknown',
          hwid: msg.hwid || 'N/A',
          ip,
          connectedAt: Date.now(),
        });
        broadcastClientList();
        return;
      }

      if (msg.type === 'command_result') {
        const payload = JSON.stringify({
          type: 'command_result',
          id: msg.id,
          clientId,
          result: msg.result,
        });
        for (const { ws: w } of clients.values()) {
          if (w.readyState === w.OPEN) w.send(payload);
        }
        return;
      }
    } catch {}
  });

  ws.on('close', () => {
    if (clientId) {
      clients.delete(clientId);
      broadcastClientList();
    }
  });
});

export default server;
