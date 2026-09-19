const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { io } = require('socket.io-client');
require('dotenv').config();

// Prevent crashes on any unhandled errors
process.on('uncaughtException', (err) => {
    console.warn('[Process] Caught exception:', err.message);
});
process.on('unhandledRejection', (reason) => {
    console.warn('[Process] Unhandled Rejection:', reason);
});

const app = express();
const PORT = process.env.PORT || 3000;
const GHOST_SECRET = process.env.GHOST_SECRET || 'ghost_secret_2026';
const SITE_URL = process.env.SITE_URL || 'https://www.arabic.chat';
const SOCKET_PATH = process.env.SOCKET_PATH || '/io/';

// Ensure data folder exists
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

const MESSAGES_FILE = path.join(DATA_DIR, 'messages.json');
const SESSION_FILE = path.join(DATA_DIR, 'session.json');

// Helper to load/save JSON
function loadJson(file, defVal) {
    try {
        if (fs.existsSync(file)) {
            const data = fs.readFileSync(file, 'utf8');
            return JSON.parse(data);
        }
    } catch (e) {
        console.warn(`[Storage] Failed to read ${file}:`, e.message);
    }
    return defVal;
}

function saveJson(file, data) {
    try {
        fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
    } catch (e) {
        console.error(`[Storage] Failed to write ${file}:`, e.message);
    }
}

let messages = loadJson(MESSAGES_FILE, []);
let sessionData = loadJson(SESSION_FILE, {
    cookies: '',
    utk: '',
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    lastUpdated: null
});

let chatSocket = null;
let socketConnected = false;
let lastConnectedTime = null;
let lastError = null;

// Clean text helpers
function stripHtml(html) {
    if (!html) return '';
    return html.replace(/<[^>]*>?/gm, '').trim();
}

function parseIncomingMessage(data) {
    if (!data) return null;
    const peerId = String(data.peer || data.target || '');
    if (!peerId || peerId === '0') return null;

    let senderName = 'مستخدم ' + peerId;
    let msgText = '';
    let msgHtml = '';
    let avatarSrc = 'default_images/avatar/default_avatar.png';

    if (data.html) {
        msgHtml = String(data.html);

        // Extract name
        const nameMatch = msgHtml.match(/class="[^"]*(?:username|user_name|target_name)[^"]*"[^>]*>([^<]+)</i);
        if (nameMatch && nameMatch[1]) {
            senderName = nameMatch[1].trim();
        }

        // Extract avatar
        const avMatch = msgHtml.match(/<img[^>]*src="([^"]+)"[^>]*class="[^"]*(?:avatar_private|private_avatar)[^"]*"/i) ||
            msgHtml.match(/class="[^"]*(?:avatar_private|private_avatar)[^"]*"[^>]*src="([^"]+)"/i) ||
            msgHtml.match(/<img[^>]*src="([^"]+)"/i);
        if (avMatch && avMatch[1]) {
            avatarSrc = avMatch[1];
        }

        // Extract content
        const contentMatch = msgHtml.match(/class="[^"]*(?:target_private|hunter_private|private_content)[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
        if (contentMatch && contentMatch[1]) {
            msgHtml = contentMatch[1].trim();
            msgText = stripHtml(msgHtml);
        } else {
            msgText = stripHtml(msgHtml);
        }
    } else if (data.message || data.msg || data.content) {
        msgText = String(data.message || data.msg || data.content);
        msgHtml = msgText;
    }

    if (!msgText && !msgHtml) return null;

    const messageId = String(data.id || ('msg_cloud_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5)));
    const timeNow = new Date().toLocaleTimeString('ar-EG', { hour: '2-digit', minute: '2-digit' });

    return {
        id: messageId,
        peerId: peerId,
        name: senderName,
        avatar: avatarSrc,
        text: msgText,
        html: msgHtml || msgText,
        time: timeNow,
        timestamp: Date.now(),
        synced: false
    };
}

// Socket Connection Handler
function initChatSocket() {
    if (chatSocket) {
        try {
            chatSocket.removeAllListeners();
            chatSocket.disconnect();
        } catch (e) { }
        chatSocket = null;
    }

    const headers = {
        'Origin': SITE_URL,
        'Referer': SITE_URL + '/',
        'User-Agent': sessionData.userAgent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    };

    if (sessionData.cookies) {
        headers['Cookie'] = sessionData.cookies;
    }

    console.log(`[Socket] Connecting to ${SITE_URL}${SOCKET_PATH}...`);
    if (sessionData.cookies) {
        console.log(`[Socket] Using authenticated session cookies (${sessionData.cookies.length} chars)`);
    } else {
        console.log(`[Socket] Warning: No session cookies configured yet. Use /api/session to supply cookies.`);
    }

    try {
        chatSocket = io(SITE_URL, {
            path: SOCKET_PATH,
            transports: ['websocket', 'polling'],
            extraHeaders: headers,
            reconnection: true,
            reconnectionDelay: 3000,
            reconnectionDelayMax: 30000,
            autoConnect: true,
            timeout: 20000
        });

        chatSocket.on('connect', () => {
            socketConnected = true;
            lastConnectedTime = new Date().toISOString();
            lastError = null;
            console.log(`[Socket] Connected successfully! Socket ID: ${chatSocket.id}`);
        });

        chatSocket.on('disconnect', (reason) => {
            socketConnected = false;
            console.log(`[Socket] Disconnected. Reason: ${reason}`);
        });

        chatSocket.on('connect_error', (err) => {
            socketConnected = false;
            lastError = err ? err.message : 'Unknown connect error';
            console.warn(`[Socket] Connection error: ${lastError}`);
        });

        chatSocket.on('error', (err) => {
            console.warn('[Socket] Generic socket error:', err);
        });

        if (chatSocket.io) {
            chatSocket.io.on('error', (err) => {
                console.warn('[Socket Manager] Engine error:', err);
            });
            chatSocket.io.on('reconnect_error', (err) => {
                console.warn('[Socket Manager] Reconnect error:', err);
            });
        }

        chatSocket.on('private-msg', (data) => {
            try {
                console.log('[Socket] Incoming private-msg received!');
                const parsed = parseIncomingMessage(data);
                if (!parsed) return;

                // Check for duplicate
                const exists = messages.some(m => m.id === parsed.id);
                if (!exists) {
                    messages.push(parsed);
                    // Keep max 500 messages
                    if (messages.length > 500) {
                        messages = messages.slice(-500);
                    }
                    saveJson(MESSAGES_FILE, messages);
                    console.log(`[Ghost Cloud] Captured message from ${parsed.name} (${parsed.peerId}): ${parsed.text.substring(0, 40)}`);
                }
            } catch (err) {
                console.error('[Socket] Error processing private-msg:', err);
            }
        });

    } catch (e) {
        lastError = e.message;
        console.error('[Socket] Exception initializing socket:', e);
    }
}

// Middleware
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Auth middleware for /api/*
function requireAuth(req, res, next) {
    const key = req.headers['x-ghost-secret'] || req.query.key || (req.body && req.body.key);
    if (!key || key !== GHOST_SECRET) {
        return res.status(401).json({ ok: false, error: 'Unauthorized: Invalid or missing secret key' });
    }
    next();
}

// 1. Web Dashboard (Home)
app.get('/', (req, res) => {
    const unsynced = messages.filter(m => !m.synced).length;
    res.send(`<!DOCTYPE html>
<html dir="rtl" lang="ar">
<head>
    <meta charset="UTF-8">
    <title>Ghost Cloud Relay Server 👻</title>
    <style>
        body { font-family: system-ui, -apple-system, sans-serif; background: #011c23; color: #fff; margin: 0; padding: 40px 20px; direction: rtl; }
        .card { max-width: 650px; margin: 0 auto; background: #022b35; border: 1px solid #00bcd4; border-radius: 12px; padding: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); }
        h1 { color: #00bcd4; margin-top: 0; font-size: 24px; display: flex; align-items: center; gap: 10px; }
        .badge { display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 13px; font-weight: bold; }
        .badge.online { background: #22c55e; color: #fff; }
        .badge.offline { background: #ef4444; color: #fff; }
        .stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 25px 0; }
        .stat-box { background: rgba(0,0,0,0.25); border-radius: 8px; padding: 15px; border-right: 4px solid #00bcd4; }
        .stat-val { font-size: 24px; font-weight: bold; color: #00e5ff; margin-top: 5px; }
        .stat-label { font-size: 13px; color: #94a3b8; }
        code { background: rgba(0,0,0,0.4); padding: 2px 6px; border-radius: 4px; font-family: monospace; color: #f59e0b; direction: ltr; display: inline-block; }
        .footer { margin-top: 25px; font-size: 12px; color: #64748b; text-align: center; border-top: 1px solid rgba(255,255,255,0.1); padding-top: 15px; }
    </style>
</head>
<body>
    <div class="card">
        <h1>👻 خادم شبح السحابي (Ghost Cloud Relay)</h1>
        <p>الخادم يعمل بنجاح 24/7 لالتقاط رسائل الخاص بصمت وتمريرها لإضافة المتصفح (Zero-Seen).</p>
        
        <div class="stat-grid">
            <div class="stat-box">
                <div class="stat-label">حالة الاتصال بموقع الشات</div>
                <div class="stat-val">
                    <span class="badge ${socketConnected ? 'online' : 'offline'}">${socketConnected ? '🟢 متصل بالسوكت' : '🔴 غير متصل'}</span>
                </div>
            </div>
            <div class="stat-box">
                <div class="stat-label">رسائل بانتظار المزامنة</div>
                <div class="stat-val">${unsynced} / ${messages.length}</div>
            </div>
        </div>

        <div style="background:rgba(0,0,0,0.2); padding:15px; border-radius:8px; font-size:14px; line-height:1.7;">
            <div><strong>🔑 المفتاح السري:</strong> محمي بواسطة <code>GHOST_SECRET</code></div>
            <div><strong>🍪 حالة كوكيز الجلسة:</strong> ${sessionData.cookies ? '✔️ مسجلة ومحدثة' : '⚠️ بانتظار المزامنة الأولى من الإضافة'}</div>
            <div><strong>🕒 آخر اتصال:</strong> ${lastConnectedTime ? new Date(lastConnectedTime).toLocaleTimeString('ar-EG') : 'لم يتصل بعد'}</div>
            ${lastError ? `<div style="color:#f87171;"><strong>⚠️ آخر خطأ:</strong> ${lastError}</div>` : ''}
        </div>

        <div class="footer">
            جاهز للمزامنة مع إضافة المتصفح Arabic Chat Ghost Extension
        </div>
    </div>
</body>
</html>`);
});

// 2. Status API
app.get('/api/status', (req, res) => {
    res.json({
        ok: true,
        socketConnected: socketConnected,
        siteUrl: SITE_URL,
        hasSession: !!sessionData.cookies,
        sessionLastUpdated: sessionData.lastUpdated,
        lastConnectedTime: lastConnectedTime,
        lastError: lastError,
        totalMessages: messages.length,
        unsyncedCount: messages.filter(m => !m.synced).length
    });
});

// 3. Sync Messages (Called by Chrome Extension)
app.get('/api/sync', requireAuth, (req, res) => {
    const markAsSynced = req.query.mark !== 'false';
    const getAll = req.query.all === 'true';
    const since = parseInt(req.query.since || '0', 10);

    let resultMsgs = [];
    if (getAll) {
        resultMsgs = since > 0 ? messages.filter(m => m.timestamp > since) : [...messages];
    } else {
        resultMsgs = messages.filter(m => !m.synced && (since === 0 || m.timestamp > since));
    }

    if (markAsSynced && resultMsgs.length > 0) {
        const idSet = new Set(resultMsgs.map(m => m.id));
        messages.forEach(m => {
            if (idSet.has(m.id)) {
                m.synced = true;
            }
        });
        saveJson(MESSAGES_FILE, messages);
    }

    res.json({
        ok: true,
        count: resultMsgs.length,
        messages: resultMsgs,
        serverTime: Date.now()
    });
});

// 4. Update Session / Cookies (Called automatically by Extension on page visit)
app.post('/api/session', requireAuth, (req, res) => {
    const { cookies, utk, userAgent } = req.body;
    let changed = false;

    if (cookies && cookies !== sessionData.cookies) {
        sessionData.cookies = cookies;
        changed = true;
    }
    if (utk && utk !== sessionData.utk) {
        sessionData.utk = utk;
        changed = true;
    }
    if (userAgent && userAgent !== sessionData.userAgent) {
        sessionData.userAgent = userAgent;
        changed = true;
    }

    sessionData.lastUpdated = new Date().toISOString();
    saveJson(SESSION_FILE, sessionData);

    console.log('[API] Session updated from extension! Reconnecting socket with fresh session...');
    initChatSocket();

    res.json({
        ok: true,
        message: 'Session stored and socket reconnecting with fresh credentials',
        socketConnected: socketConnected
    });
});

// 5. Test Message Injector (For debugging and manual verification)
app.post('/api/test-msg', requireAuth, (req, res) => {
    const { peer, name, message } = req.body;
    const testPeer = String(peer || '9999');
    const testName = name || 'تجربة سحابية 👻';
    const testText = message || 'هذه رسالة واردة تم التقاطها عبر السيرفر السحابي أثناء إغلاق المتصفح!';

    const testMsg = {
        id: 'msg_test_' + Date.now(),
        peerId: testPeer,
        name: testName,
        avatar: 'default_images/avatar/default_avatar.png',
        text: testText,
        html: `<div class="target_private">${testText}</div>`,
        time: new Date().toLocaleTimeString('ar-EG', { hour: '2-digit', minute: '2-digit' }),
        timestamp: Date.now(),
        synced: false
    };

    messages.push(testMsg);
    saveJson(MESSAGES_FILE, messages);

    res.json({
        ok: true,
        message: 'Test message added to cloud storage',
        data: testMsg
    });
});

// 6. Clear Messages API
app.post('/api/clear', requireAuth, (req, res) => {
    messages = [];
    saveJson(MESSAGES_FILE, messages);
    res.json({ ok: true, message: 'All messages cleared' });
});

// Start Server
app.listen(PORT, '0.0.0.0', () => {
    console.log(`=================================================`);
    console.log(`👻 Ghost Cloud Relay Server is RUNNING on port ${PORT}`);
    console.log(`🔒 Secret Key: ${GHOST_SECRET}`);
    console.log(`🌐 Target: ${SITE_URL}${SOCKET_PATH}`);
    console.log(`=================================================`);

    // Initial socket connect
    initChatSocket();
});
