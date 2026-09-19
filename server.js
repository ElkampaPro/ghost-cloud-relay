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
        type: 'received',
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
                    if (messages.length > 1000) {
                        messages = messages.slice(-1000);
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

// ==========================================
// 24/7 Background HTTP Poller Engine
// ==========================================
let pollIntervalTimer = null;
let isPollingActive = false;
let lastPollStats = {
    lastPollTime: null,
    status: 'idle',
    lastUnreadFound: 0,
    totalCycles: 0,
    lastError: null
};

async function pollArabicChatOnce() {
    if (!sessionData.utk && !sessionData.cookies) {
        lastPollStats.status = 'no_credentials';
        return;
    }
    if (isPollingActive) return;
    isPollingActive = true;

    try {
        const headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': sessionData.userAgent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'Origin': SITE_URL,
            'Referer': `${SITE_URL}/`
        };
        if (sessionData.cookies) {
            headers['Cookie'] = sessionData.cookies;
        }

        const bodyData = sessionData.utk ? `token=${encodeURIComponent(sessionData.utk)}` : '';

        // 1. Poll private_notify.php
        const notifyRes = await fetch(`${SITE_URL}/system/box/private_notify.php`, {
            method: 'POST',
            headers: headers,
            body: bodyData
        });

        if (!notifyRes.ok) {
            lastPollStats.status = `HTTP_${notifyRes.status}`;
            isPollingActive = false;
            return;
        }

        const notifyHtml = await notifyRes.text();
        lastPollStats.lastPollTime = new Date().toISOString();
        lastPollStats.totalCycles++;
        lastPollStats.status = 'ok';

        if (!notifyHtml || notifyHtml.trim().length === 0) {
            isPollingActive = false;
            return;
        }

        // 2. Parse contacts from notifyHtml
        const rawBlocks = notifyHtml.split(/(?=<div[^>]*class="[^"]*ulist_item)/i);
        const peersToCheck = new Map();

        for (const block of rawBlocks) {
            const peerMatch = block.match(/data="(\d+)"/i);
            if (!peerMatch) continue;
            const peerId = peerMatch[1];
            if (!peerId || peerId === '0') continue;

            let name = 'مستخدم ' + peerId;
            const valMatch = block.match(/value="([^"]+)"/i);
            if (valMatch && valMatch[1]) {
                name = valMatch[1].trim();
            } else {
                const nameMatch = block.match(/class="[^"]*(?:username|user_name)[^"]*"[^>]*>([^<]+)</i);
                if (nameMatch && nameMatch[1]) name = nameMatch[1].trim();
            }

            let avatar = 'default_images/avatar/default_avatar.png';
            const avMatch = block.match(/data-av="([^"]+)"/i) || block.match(/<img[^>]*src="([^"]+)"/i);
            if (avMatch && avMatch[1]) avatar = avMatch[1];

            const unreadMatch = block.match(/class="[^"]*pm_notify[^"]*"[^>]*>(\d+)</i);
            const unreadCount = unreadMatch ? parseInt(unreadMatch[1], 10) : 0;

            peersToCheck.set(peerId, { peerId, name, avatar, unreadCount });
        }

        // 2. Parse contacts from notifyHtml (Read-only, Zero-Seen compliant)
        // Strictly NEVER fetch system/private_box.php here: private_box.php updates seen=1 on the server!
        for (const [peerId, info] of peersToCheck.entries()) {
            // Update names and avatars for existing stored messages if improved info was parsed
            messages.forEach(m => {
                if (m.peerId === peerId) {
                    if (info.name && m.name === ('مستخدم ' + peerId)) m.name = info.name;
                    if (info.avatar && m.avatar === 'default_images/avatar/default_avatar.png') m.avatar = info.avatar;
                }
            });

            if (info.unreadCount > 0) {
                lastPollStats.lastUnreadFound = info.unreadCount;
                console.log(`[Cloud Poller] 👻 Unread private notification from ${info.name} (${peerId}): ${info.unreadCount} unread message(s) [Zero-Seen active, no HTTP write]`);
                
                // If socket is disconnected while unread messages are waiting, trigger reconnect to receive private-msg
                if (!socketConnected) {
                    console.log(`[Cloud Poller] Socket disconnected. Re-initializing socket listener for real-time capture...`);
                    initChatSocket();
                }
            }
        }

    } catch (err) {
        lastPollStats.lastError = err.message;
        console.warn('[Cloud Poller] Polling cycle error:', err.message);
    } finally {
        isPollingActive = false;
    }
}

function startPollingEngine() {
    if (pollIntervalTimer) clearInterval(pollIntervalTimer);
    console.log('[Cloud Poller] Starting 24/7 background polling engine (interval: 5s)...');
    setTimeout(pollArabicChatOnce, 1500);
    pollIntervalTimer = setInterval(pollArabicChatOnce, 5000);
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

// 1. Web Dashboard & Mobile Web Chat App (Home)
app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html>
<html dir="rtl" lang="ar">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>شبح الخاص | Ghost Cloud Chat 👻</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }
        body { font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #011c23; color: #fff; height: 100vh; display: flex; flex-direction: column; overflow: hidden; direction: rtl; }
        
        /* Login Overlay */
        #login_overlay { position: fixed; inset: 0; background: #011c23; z-index: 1000; display: flex; align-items: center; justify-content: center; padding: 20px; }
        .login_card { background: #022b35; border: 1px solid #00bcd4; border-radius: 14px; padding: 30px 24px; width: 100%; max-width: 400px; text-align: center; box-shadow: 0 10px 30px rgba(0,0,0,0.6); }
        .login_card h2 { color: #00bcd4; margin-bottom: 8px; font-size: 22px; display: flex; align-items: center; justify-content: center; gap: 8px; }
        .login_card p { color: #94a3b8; font-size: 13px; margin-bottom: 20px; line-height: 1.5; }
        .login_input { width: 100%; padding: 12px 14px; border-radius: 8px; border: 1px solid #00bcd4; background: rgba(0,0,0,0.3); color: #fff; font-size: 14px; direction: ltr; text-align: center; margin-bottom: 16px; outline: none; }
        .login_btn { width: 100%; padding: 12px; background: #00bcd4; color: #012832; border: none; border-radius: 8px; font-weight: bold; font-size: 15px; cursor: pointer; transition: 0.2s; }
        .login_btn:hover { background: #00e5ff; }

        /* Main App Header */
        header { height: 50px; background: rgb(1, 40, 50); border-bottom: 1px solid #00bcd4; display: flex; align-items: center; justify-content: space-between; padding: 0 16px; flex-shrink: 0; }
        .header_title { font-size: 16px; font-weight: bold; color: #00bcd4; display: flex; align-items: center; gap: 8px; }
        .badge_status { font-size: 11px; padding: 3px 8px; border-radius: 12px; background: #10b981; color: #fff; font-weight: bold; }
        .badge_status.offline { background: #ef4444; }
        .header_actions { display: flex; align-items: center; gap: 12px; }
        .header_btn { background: none; border: none; color: #94a3b8; font-size: 16px; cursor: pointer; }
        .header_btn:hover { color: #fff; }

        /* App Container */
        #app_container { flex: 1; display: flex; overflow: hidden; position: relative; }
        
        /* Contacts Sidebar / Pane */
        #contacts_pane { width: 340px; background: #02232b; border-left: 1px solid rgba(0,188,212,0.2); display: flex; flex-direction: column; flex-shrink: 0; }
        .search_bar { padding: 10px 12px; border-bottom: 1px solid rgba(255,255,255,0.08); }
        .search_input { width: 100%; padding: 8px 12px; border-radius: 20px; border: 1px solid rgba(0,188,212,0.3); background: rgba(0,0,0,0.25); color: #fff; font-size: 13px; outline: none; }
        #contacts_list { flex: 1; overflow-y: auto; list-style: none; }
        .contact_item { display: flex; align-items: center; gap: 12px; padding: 12px 14px; border-bottom: 1px solid rgba(255,255,255,0.05); cursor: pointer; transition: background 0.15s; }
        .contact_item:hover, .contact_item.active { background: rgba(0, 188, 212, 0.12); }
        .contact_avatar { width: 44px; height: 44px; border-radius: 50%; object-fit: cover; border: 1.5px solid #00bcd4; flex-shrink: 0; }
        .contact_info { flex: 1; min-width: 0; }
        .contact_header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px; }
        .contact_name { font-size: 14px; font-weight: bold; color: #fff; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .contact_time { font-size: 11px; color: #94a3b8; }
        .contact_snippet { font-size: 12px; color: #94a3b8; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

        /* Chat Messages Pane */
        #chat_pane { flex: 1; display: flex; flex-direction: column; background: #01181e; position: relative; }
        #chat_top { height: 50px; background: rgba(1, 40, 50, 0.95); border-bottom: 1px solid rgba(0,188,212,0.2); display: flex; align-items: center; justify-content: space-between; padding: 0 16px; }
        .chat_target_info { display: flex; align-items: center; gap: 10px; }
        .back_btn { display: none; background: none; border: none; color: #00bcd4; font-size: 18px; cursor: pointer; margin-left: 8px; }
        
        #messages_area { flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 10px; }
        
        /* Message Bubbles matching Arabic Chat exact colors */
        .msg_row { display: flex; align-items: flex-end; gap: 8px; max-width: 82%; }
        .msg_row.received { align-self: flex-start; }
        .msg_row.sent { align-self: flex-end; flex-direction: row-reverse; }
        .msg_bubble { padding: 8px 12px; border-radius: 8px; font-size: 13.5px; line-height: 1.45; word-break: break-word; }
        .msg_row.received .msg_bubble { background: rgb(51, 51, 51); color: #fff; border-radius: 6px 0 6px 6px; }
        .msg_row.sent .msg_bubble { background: rgb(0, 188, 212); color: #012832; font-weight: 500; border-radius: 0 6px 6px 6px; }
        .msg_meta { font-size: 10px; opacity: 0.7; margin-top: 4px; text-align: left; }
        .msg_row.received .msg_meta { text-align: right; }
        
        /* Audio and Media */
        audio { height: 34px; max-width: 220px; outline: none; margin-top: 4px; }
        .msg_bubble img { max-width: 100%; max-height: 240px; border-radius: 6px; cursor: pointer; }

        /* Empty placeholder */
        .empty_view { margin: auto; text-align: center; color: #64748b; padding: 30px; }
        .empty_view i { font-size: 44px; color: #00bcd4; margin-bottom: 12px; display: block; opacity: 0.6; }

        /* Responsive Breakpoints for Mobile Phones */
        @media (max-width: 768px) {
            #contacts_pane { width: 100%; position: absolute; inset: 0; z-index: 10; }
            #chat_pane { width: 100%; position: absolute; inset: 0; display: none; z-index: 20; }
            .back_btn { display: inline-block; }
            #app_container.in_chat #contacts_pane { display: none; }
            #app_container.in_chat #chat_pane { display: flex; }
        }
    </style>
</head>
<body>

    <!-- Login Overlay -->
    <div id="login_overlay">
        <div class="login_card">
            <h2><i class="fa fa-ghost"></i> شبح الخاص السحابي</h2>
            <p>أدخل المفتاح السري (Secret Key) لفتح ومزامنة محادثات الشات الخاصة بك مباشرة على هاتفك.</p>
            <input type="password" id="secret_input" class="login_input" placeholder="أدخل المفتاح السري هنا...">
            <button id="login_btn" class="login_btn"><i class="fa fa-unlock-alt"></i> تسجيل الدخول والمزامنة</button>
        </div>
    </div>

    <!-- Main App Header -->
    <header>
        <div class="header_title">
            <span>👻 شبح الخاص (Ghost Web Viewer)</span>
            <span id="socket_badge" class="badge_status ${socketConnected ? '' : 'offline'}">${socketConnected ? '🟢 متصل بالسيرفر' : '🟡 يعمل بالسحابة'}</span>
        </div>
        <div class="header_actions">
            <button id="refresh_btn" class="header_btn" title="تحديث فوري"><i class="fa fa-sync-alt"></i></button>
            <button id="logout_btn" class="header_btn" title="تسجيل الخروج"><i class="fa fa-sign-out-alt"></i></button>
        </div>
    </header>

    <!-- App Container -->
    <div id="app_container">
        <!-- Contacts List Pane -->
        <div id="contacts_pane">
            <div class="search_bar">
                <input type="text" id="search_input" class="search_input" placeholder="🔍 بحث في جهات الاتصال...">
            </div>
            <ul id="contacts_list">
                <div class="empty_view"><i class="fa fa-spinner fa-spin"></i>جاري جلب المحادثات...</div>
            </ul>
        </div>

        <!-- Messages Pane -->
        <div id="chat_pane">
            <div id="chat_top">
                <div class="chat_target_info">
                    <button id="back_btn" class="back_btn"><i class="fa fa-arrow-right"></i></button>
                    <img id="active_avatar" class="contact_avatar" src="https://www.arabic.chat/default_images/avatar/default_avatar.png">
                    <div>
                        <div id="active_name" style="font-weight:bold; font-size:14px;">مستخدم</div>
                        <div style="font-size:11px; color:#00bcd4;">محادثة متزامنة مع الكمبيوتر</div>
                    </div>
                </div>
            </div>

            <div id="messages_area">
                <div class="empty_view">
                    <i class="fa fa-comments"></i>
                    اختر محادثة من القائمة لعرض الرسائل المتبادلة
                </div>
            </div>
        </div>
    </div>

    <script>
        let SECRET_KEY = localStorage.getItem('ghost_secret_key') || '';
        let conversations = [];
        let activePeerId = null;

        const loginOverlay = document.getElementById('login_overlay');
        const secretInput = document.getElementById('secret_input');
        const loginBtn = document.getElementById('login_btn');
        const appContainer = document.getElementById('app_container');
        const contactsList = document.getElementById('contacts_list');
        const searchInput = document.getElementById('search_input');
        const messagesArea = document.getElementById('messages_area');
        const backBtn = document.getElementById('back_btn');
        const activeName = document.getElementById('active_name');
        const activeAvatar = document.getElementById('active_avatar');
        const refreshBtn = document.getElementById('refresh_btn');
        const logoutBtn = document.getElementById('logout_btn');

        if (SECRET_KEY) {
            loginOverlay.style.display = 'none';
            initApp();
        }

        loginBtn.onclick = function () {
            const key = secretInput.value.trim();
            if (!key) return alert("يرجى كتابة المفتاح السري");
            SECRET_KEY = key;
            localStorage.setItem('ghost_secret_key', key);
            loginOverlay.style.display = 'none';
            initApp();
        };

        logoutBtn.onclick = function () {
            if (confirm("تسجيل الخروج من لوحة الشبح؟")) {
                localStorage.removeItem('ghost_secret_key');
                location.reload();
            }
        };

        backBtn.onclick = function () {
            appContainer.classList.remove('in_chat');
            activePeerId = null;
        };

        refreshBtn.onclick = function () {
            fetchConversations();
        };

        function initApp() {
            fetchConversations();
            setInterval(fetchConversations, 3500);
        }

        function fetchConversations() {
            if (!SECRET_KEY) return;
            fetch('/api/conversations?key=' + encodeURIComponent(SECRET_KEY))
                .then(r => {
                    if (r.status === 401) {
                        localStorage.removeItem('ghost_secret_key');
                        loginOverlay.style.display = 'flex';
                        throw new Error("Invalid Secret");
                    }
                    return r.json();
                })
                .then(res => {
                    if (res && res.ok) {
                        conversations = res.conversations || [];
                        renderContactsList();
                        if (activePeerId) {
                            renderActiveMessages();
                        }
                    }
                })
                .catch(err => {
                    console.warn("Sync error:", err);
                });
        }

        function renderContactsList() {
            const q = (searchInput.value || '').trim().toLowerCase();
            const filtered = conversations.filter(c => !q || (c.name && c.name.toLowerCase().includes(q)) || c.peerId.includes(q));

            if (filtered.length === 0) {
                contactsList.innerHTML = '<div class="empty_view"><i class="fa fa-inbox"></i>لا توجد محادثات مسجلة بعد</div>';
                return;
            }

            contactsList.innerHTML = filtered.map(c => {
                const isActive = c.peerId === activePeerId;
                const avatar = c.avatar.startsWith('http') ? c.avatar : ('https://www.arabic.chat/' + c.avatar.replace(/^\\/+/, ''));
                return \`
                    <li class="contact_item \${isActive ? 'active' : ''}" onclick="openChat('\${c.peerId}')">
                        <img class="contact_avatar" src="\${avatar}" onerror="this.src='https://www.arabic.chat/default_images/avatar/default_avatar.png'">
                        <div class="contact_info">
                            <div class="contact_header">
                                <span class="contact_name">\${c.name}</span>
                                <span class="contact_time">\${c.lastTime || ''}</span>
                            </div>
                            <div class="contact_snippet">\${c.lastText || 'رسالة خاصة'}</div>
                        </div>
                    </li>
                \`;
            }).join('');
        }

        window.openChat = function (peerId) {
            activePeerId = peerId;
            appContainer.classList.add('in_chat');
            const target = conversations.find(c => c.peerId === peerId);
            if (target) {
                activeName.textContent = target.name || ('مستخدم ' + peerId);
                activeAvatar.src = target.avatar.startsWith('http') ? target.avatar : ('https://www.arabic.chat/' + target.avatar.replace(/^\\/+/, ''));
            }
            renderContactsList();
            renderActiveMessages(true);
        };

        function renderActiveMessages(scrollBottom = false) {
            if (!activePeerId) return;
            const target = conversations.find(c => c.peerId === activePeerId);
            if (!target || !target.messages || target.messages.length === 0) {
                messagesArea.innerHTML = '<div class="empty_view"><i class="fa fa-comment-slash"></i>لا توجد رسائل سابقة في هذه المحادثة</div>';
                return;
            }

            messagesArea.innerHTML = target.messages.map(m => {
                const isSent = m.type === 'sent';
                let content = m.html || m.text || '';
                // Fix relative images or audio URLs
                content = content.replace(/src="(?!(https?:|blob:|data:))\\/?([^"]+)"/g, 'src="https://www.arabic.chat/$2"');

                return \`
                    <div class="msg_row \${isSent ? 'sent' : 'received'}">
                        <div class="msg_bubble">
                            \${content}
                            <div class="msg_meta">\${m.time || ''} \${isSent ? '✓✓' : ''}</div>
                        </div>
                    </div>
                \`;
            }).join('');

            if (scrollBottom) {
                messagesArea.scrollTop = messagesArea.scrollHeight;
            }
        }

        searchInput.oninput = function () {
            renderContactsList();
        };
    </script>
</body>
</html>`);
});

// 2. Status API
app.get('/api/status', (req, res) => {
    res.json({
        ok: true,
        socketConnected: socketConnected,
        siteUrl: SITE_URL,
        hasSession: !!(sessionData.utk || sessionData.cookies),
        sessionUtkPresent: !!sessionData.utk,
        sessionCookiesPresent: !!sessionData.cookies,
        sessionLastUpdated: sessionData.lastUpdated,
        lastConnectedTime: lastConnectedTime,
        lastError: lastError,
        pollerActive: true,
        lastPollTime: lastPollStats.lastPollTime,
        lastPollStatus: lastPollStats.status,
        lastPollCycles: lastPollStats.totalCycles,
        totalMessages: messages.length,
        unsyncedCount: messages.filter(m => !m.synced).length
    });
});

// 3. Sync Messages (Multi-Device Safe: PC, Kiwi Mobile, PWA)
app.get('/api/sync', requireAuth, (req, res) => {
    const markAsSynced = req.query.mark === 'true'; // Only mark if explicitly asked
    const getAll = req.query.all !== 'false'; // Default to TRUE so all devices get full sync
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

// 4. Record Sent Message from PC (Two-Way Sync)
app.post('/api/messages/sent', requireAuth, (req, res) => {
    const { peerId, name, message } = req.body;
    if (!peerId || !message) {
        return res.status(400).json({ ok: false, error: 'Missing peerId or message' });
    }

    const sentItem = {
        id: message.id || ('msg_sent_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5)),
        peerId: String(peerId),
        name: name || ('مستخدم ' + peerId),
        avatar: message.avatar || 'default_images/avatar/default_avatar.png',
        text: message.text || stripHtml(message.html || ''),
        html: message.html || message.text || '',
        time: message.time || new Date().toLocaleTimeString('ar-EG', { hour: '2-digit', minute: '2-digit' }),
        timestamp: message.timestamp || Date.now(),
        type: 'sent',
        synced: true
    };

    const exists = messages.some(m => m.id === sentItem.id);
    if (!exists) {
        messages.push(sentItem);
        if (messages.length > 1000) {
            messages = messages.slice(-1000);
        }
        saveJson(MESSAGES_FILE, messages);
        console.log(`[Ghost Cloud] Two-Way Sync: Recorded sent message to ${sentItem.name} (${sentItem.peerId})`);
    }

    res.json({ ok: true, data: sentItem });
});

// 4.1 Record Incoming Message from Extension (Two-Way Sync)
app.post('/api/messages/incoming', requireAuth, (req, res) => {
    const { peerId, name, message } = req.body;
    if (!peerId || !message) {
        return res.status(400).json({ ok: false, error: 'Missing peerId or message' });
    }

    const incomingItem = {
        id: message.id || ('msg_in_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5)),
        peerId: String(peerId),
        name: name || ('مستخدم ' + peerId),
        avatar: message.avatar || 'default_images/avatar/default_avatar.png',
        text: message.text || stripHtml(message.html || ''),
        html: message.html || message.text || '',
        time: message.time || new Date().toLocaleTimeString('ar-EG', { hour: '2-digit', minute: '2-digit' }),
        timestamp: message.timestamp || Date.now(),
        type: 'received',
        synced: true // Already handled by the reporting extension
    };

    const exists = messages.some(m => m.id === incomingItem.id || (m.peerId === incomingItem.peerId && m.type === 'received' && (m.text === incomingItem.text || m.html === incomingItem.html) && m.time === incomingItem.time));
    if (!exists) {
        messages.push(incomingItem);
        if (messages.length > 1500) {
            messages = messages.slice(-1500);
        }
        saveJson(MESSAGES_FILE, messages);
        console.log(`[Ghost Cloud] Two-Way Sync: Recorded incoming message from ${incomingItem.name} (${incomingItem.peerId})`);
    }

    res.json({ ok: true, data: incomingItem });
});

// 5. Get Full Conversations (For Mobile Web Viewer)
app.get('/api/conversations', requireAuth, (req, res) => {
    const map = {};
    messages.forEach(m => {
        if (!map[m.peerId]) {
            map[m.peerId] = {
                peerId: m.peerId,
                name: m.name || ('مستخدم ' + m.peerId),
                avatar: m.avatar || 'default_images/avatar/default_avatar.png',
                lastTime: m.time,
                lastTimestamp: m.timestamp,
                lastText: m.text,
                messages: []
            };
        }
        if (m.name && m.name !== ('مستخدم ' + m.peerId)) {
            map[m.peerId].name = m.name;
        }
        if (m.avatar && m.avatar !== 'default_images/avatar/default_avatar.png') {
            map[m.peerId].avatar = m.avatar;
        }
        map[m.peerId].lastTime = m.time;
        map[m.peerId].lastTimestamp = m.timestamp;
        map[m.peerId].lastText = m.text;
        map[m.peerId].messages.push(m);
    });

    const convs = Object.values(map).sort((a, b) => (b.lastTimestamp || 0) - (a.lastTimestamp || 0));
    res.json({ ok: true, count: convs.length, conversations: convs });
});

// 6. Update Session / Cookies (Called automatically by Extension on page visit)
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
    setTimeout(pollArabicChatOnce, 300);

    res.json({
        ok: true,
        message: 'Session stored and socket reconnecting with fresh credentials',
        socketConnected: socketConnected
    });
});

// 7. Test Message Injector (For debugging and manual verification)
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
        type: 'received',
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

// 8. Clear Messages API
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

    // Initial socket connect & Polling Engine boot
    initChatSocket();
    startPollingEngine();
});
