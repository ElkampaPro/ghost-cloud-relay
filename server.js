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
const GHOST_SECRET = (process.env.GHOST_SECRET || '').trim();
if (!GHOST_SECRET || GHOST_SECRET === 'ghost_secret_2026') {
    console.warn('[SECURITY WARNING] GHOST_SECRET is not configured or uses weak default. Authentication will fail closed until a valid non-default GHOST_SECRET is set.');
}
const SITE_URL = process.env.SITE_URL || 'https://www.arabic.chat';
const SOCKET_PATH = process.env.SOCKET_PATH || '/io/';

// Ensure data folder exists (configurable via DATA_DIR for persistent volume mounts e.g. /data on Render)
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

const MESSAGES_FILE = path.join(DATA_DIR, 'messages.json');
const SESSION_FILE = path.join(DATA_DIR, 'session.json');
const ACCOUNTS_FILE = path.join(DATA_DIR, 'accounts.json');
const TRANSACTION_FILE = path.join(DATA_DIR, 'transaction.json');
const DELETION_CUTOFFS_FILE = path.join(DATA_DIR, 'deletion_cutoffs.json');
const DEVICE_CURSORS_FILE = path.join(DATA_DIR, 'device_cursors.json');
const EPOCH_FILE = path.join(DATA_DIR, 'epoch.json');
const PID_FILE = path.join(__dirname, 'server.pid');

let storageHealth = { ok: true, error: null, lastIncident: null };

try {
    fs.writeFileSync(PID_FILE, String(process.pid));
} catch (e) {}

process.on('exit', () => {
    try { if (fs.existsSync(PID_FILE)) fs.unlinkSync(PID_FILE); } catch (e) {}
});

// In-Memory Ring Buffer for Logs
const serverLogs = [];
function addLog(msg) {
    const timestamp = new Date().toISOString().substring(11, 19);
    const line = `[${timestamp}] ${msg}`;
    console.log(line);
    serverLogs.push(line);
    if (serverLogs.length > 250) serverLogs.shift();
}

// Helper to load/save JSON
function loadJson(file, defVal) {
    try {
        if (fs.existsSync(file)) {
            const data = fs.readFileSync(file, 'utf8');
            return JSON.parse(data);
        }
    } catch (e) {
        console.error(`[Storage] CRITICAL: Failed to parse ${file}: ${e.message}. Preserving original file to avoid data loss.`);
        try {
            const backup = `${file}.corrupt.${Date.now()}`;
            fs.copyFileSync(file, backup);
            console.warn(`[Storage] Corrupt file backed up to ${backup}`);
        } catch (bErr) {}
    }
    return defVal;
}

function saveJson(file, data) {
    let tmpFile = null;
    try {
        const dir = path.dirname(file);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
        tmpFile = path.join(dir, `.tmp_${path.basename(file)}_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`);
        fs.writeFileSync(tmpFile, JSON.stringify(data, null, 2), 'utf8');
        fs.renameSync(tmpFile, file);
        return true;
    } catch (e) {
        if (tmpFile) {
            try { if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile); } catch (uErr) {}
        }
        console.error(`[Storage] Failed to write ${file}:`, e.message);
        return false;
    }
}

// Message Retention Policy
const DEFAULT_MAX_MESSAGES = 5000;
const MAX_MESSAGES = parseInt(process.env.GHOST_MAX_MESSAGES || '', 10) || DEFAULT_MAX_MESSAGES;

function applyRetentionPolicy(candidateMessages) {
    if (!Array.isArray(candidateMessages)) return [];
    if (candidateMessages.length <= MAX_MESSAGES) {
        return candidateMessages;
    }
    const dropCount = candidateMessages.length - MAX_MESSAGES;
    const trimmed = candidateMessages.slice(-MAX_MESSAGES);
    const line = `[RetentionPolicy] Global retention capacity reached (${candidateMessages.length}/${MAX_MESSAGES}). Trimmed ${dropCount} oldest message(s) under FIFO policy.`;
    addLog(line);
    console.warn(line);
    return trimmed;
}

let messages = loadJson(MESSAGES_FILE, []);
let deletionCutoffs = loadJson(DELETION_CUTOFFS_FILE, {});
let deviceCursors = loadJson(DEVICE_CURSORS_FILE, {});

let epochData = loadJson(EPOCH_FILE, null);
if (!epochData || !epochData.epoch) {
    epochData = { epoch: Date.now() };
    saveJson(EPOCH_FILE, epochData);
}
const SERVER_EPOCH = epochData.epoch;

const ownerSeqCounters = {};
let messagesUpdatedWithSeq = false;
if (Array.isArray(messages)) {
    messages.forEach(m => {
        const owner = m.owner || 'default';
        if (!ownerSeqCounters[owner]) ownerSeqCounters[owner] = 0;
        if (typeof m.seq === 'number' && Number.isFinite(m.seq)) {
            if (m.seq > ownerSeqCounters[owner]) {
                ownerSeqCounters[owner] = m.seq;
            }
        }
    });
    messages.forEach(m => {
        const owner = m.owner || 'default';
        if (typeof m.seq !== 'number' || !Number.isFinite(m.seq)) {
            ownerSeqCounters[owner] = (ownerSeqCounters[owner] || 0) + 1;
            m.seq = ownerSeqCounters[owner];
            messagesUpdatedWithSeq = true;
        }
    });
    if (messagesUpdatedWithSeq) {
        saveJson(MESSAGES_FILE, messages);
    }
}

function nextOwnerSeq(owner) {
    const key = owner || 'default';
    ownerSeqCounters[key] = (ownerSeqCounters[key] || 0) + 1;
    return ownerSeqCounters[key];
}

function isPeerTombstoned(owner, peerId, timestamp) {
    if (!owner || !peerId) return false;
    const key = `${owner}:${peerId}`;
    const cutoff = Number(deletionCutoffs[key] || 0);
    if (cutoff > 0 && Number(timestamp || 0) <= cutoff) {
        return true;
    }
    return false;
}

function recordIncomingToPendingTransaction(item) {
    if (!item || !item.id) return;
    try {
        if (fs.existsSync(TRANSACTION_FILE)) {
            const tx = loadJson(TRANSACTION_FILE, null);
            if (tx && tx.status && tx.status !== 'committed' && tx.previous && Array.isArray(tx.previous.messages)) {
                if (!tx.previous.messages.some(m => m.id === item.id)) {
                    tx.previous.messages.push(item);
                    saveJson(TRANSACTION_FILE, tx);
                }
            }
        }
    } catch (_) {}
}

function getCompositeMessageKey(m) {
    if (!m) return '';
    const id = m.id !== undefined && m.id !== null ? String(m.id) : '';
    const owner = m.owner || '';
    const peerId = m.peerId !== undefined && m.peerId !== null ? String(m.peerId) : '';
    const type = m.type || '';
    return `${owner}:::${peerId}:::${type}:::${id}`;
}

function reconcileRecoveredMessagesList(previousJournalMessages, currentDiskMessages) {
    const cutoffs = loadJson(DELETION_CUTOFFS_FILE, deletionCutoffs || {});
    function isTombstonedRecovery(m) {
        if (!m || !m.owner || !m.peerId) return false;
        const key = `${m.owner}:${m.peerId}`;
        const cutoff = Number(cutoffs[key] || 0);
        return cutoff > 0 && Number(m.timestamp || 0) <= cutoff;
    }

    const mergedMessagesMap = new Map();
    for (const m of (currentDiskMessages || [])) {
        if (m && !isTombstonedRecovery(m)) {
            const k = getCompositeMessageKey(m);
            if (k) mergedMessagesMap.set(k, m);
        }
    }
    for (const m of (previousJournalMessages || [])) {
        if (m && !isTombstonedRecovery(m)) {
            const k = getCompositeMessageKey(m);
            if (k && !mergedMessagesMap.has(k)) {
                mergedMessagesMap.set(k, m);
            }
        }
    }
    return applyRetentionPolicy(Array.from(mergedMessagesMap.values()));
}

let sessionData = loadJson(SESSION_FILE, {
    cookies: process.env.GHOST_COOKIES || '',
    utk: process.env.GHOST_UTK || '',
    userAgent: process.env.GHOST_USER_AGENT || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    lastUpdated: null
});

if (!sessionData.cookies && process.env.GHOST_COOKIES) {
    sessionData.cookies = process.env.GHOST_COOKIES;
}
if (!sessionData.utk && process.env.GHOST_UTK) {
    sessionData.utk = process.env.GHOST_UTK;
}

let chatSocket = null;
let socketConnected = false;
let lastConnectedTime = null;
let lastError = null;
let socketConnectingStartedAt = null;

function extractExplicitUserId(cookies) {
    if (cookies && typeof cookies === 'string') {
        const u = cookies.match(/(?:user_id|my_id)=([^;]+)/i);
        if (u) return u[1].trim();
    }
    return null;
}

function extractStableAccountId(cookies, utk) {
    if (cookies && typeof cookies === 'string') {
        const u = cookies.match(/(?:user_id|my_id)=([^;]+)/i);
        if (u) return u[1].trim();
        const p = cookies.match(/PHPSESSID=([^;]+)/i);
        if (p) {
            try {
                const nodeCrypto = require('crypto');
                return 'phpsess_' + nodeCrypto.createHash('sha256').update(p[1].trim()).digest('hex').slice(0, 12);
            } catch (e) {
                let h = 0;
                const s = p[1].trim();
                for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
                return 'phpsess_' + Math.abs(h).toString(16).padStart(8, '0');
            }
        }
    }
    if (utk && typeof utk === 'string' && utk.trim()) {
        try {
            const nodeCrypto = require('crypto');
            return 'utk_' + nodeCrypto.createHash('sha256').update(utk.trim()).digest('hex').slice(0, 12);
        } catch (e) {
            let h = 0;
            const s = utk.trim();
            for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
            return 'utk_' + Math.abs(h).toString(16).padStart(8, '0');
        }
    }
    return null;
}

// Account Sessions Registry (for multi-tenant and concurrent client session isolation)
let accountSessions = loadJson(ACCOUNTS_FILE, {});

// 1. Transaction Journal Recovery on Startup (restores pre-transaction consistent state if a crash or rollback failure occurred)
try {
    if (fs.existsSync(TRANSACTION_FILE)) {
        const tx = loadJson(TRANSACTION_FILE, null);
        if (tx && tx.status && tx.status !== 'committed' && tx.previous) {
            console.warn(`[Startup Recovery] Uncommitted transaction ${tx.id} (${tx.status}) detected. Reconciling disk files...`);
            let allRestored = true;
            let recoveryError = null;

            if (tx.previous.deletionCutoffs) {
                if (saveJson(DELETION_CUTOFFS_FILE, tx.previous.deletionCutoffs)) {
                    deletionCutoffs = tx.previous.deletionCutoffs;
                } else {
                    allRestored = false;
                    recoveryError = 'Failed to restore deletion_cutoffs.json';
                }
            }
            if (tx.previous.accounts) {
                if (saveJson(ACCOUNTS_FILE, tx.previous.accounts)) {
                    accountSessions = tx.previous.accounts;
                } else {
                    allRestored = false;
                    recoveryError = (recoveryError ? recoveryError + '; ' : '') + 'Failed to restore accounts.json';
                }
            }
            if (tx.previous.messages) {
                // When restoring messages from an uncommitted transaction:
                // Reconcile disk messages and journal messages with deletion cutoffs and composite message identity
                const currentDiskMessages = loadJson(MESSAGES_FILE, messages || []);
                const mergedMessages = reconcileRecoveredMessagesList(tx.previous.messages, currentDiskMessages);
                if (saveJson(MESSAGES_FILE, mergedMessages)) {
                    messages = mergedMessages;
                } else {
                    allRestored = false;
                    recoveryError = (recoveryError ? recoveryError + '; ' : '') + 'Failed to restore messages.json';
                }
            }
            if (tx.previous.session) {
                if (saveJson(SESSION_FILE, tx.previous.session)) {
                    sessionData = tx.previous.session;
                } else {
                    allRestored = false;
                    recoveryError = (recoveryError ? recoveryError + '; ' : '') + 'Failed to restore session.json';
                }
            }

            if (allRestored) {
                // Verify all restored files exist and are valid JSON on disk before clearing journal
                let verified = true;
                if (tx.previous.accounts && !loadJson(ACCOUNTS_FILE, null)) verified = false;
                if (tx.previous.messages && !loadJson(MESSAGES_FILE, null)) verified = false;
                if (tx.previous.session && !loadJson(SESSION_FILE, null)) verified = false;

                if (verified) {
                    console.log(`[Startup Recovery] Successfully reconciled all files for transaction ${tx.id}. Clearing journal.`);
                    try { fs.unlinkSync(TRANSACTION_FILE); } catch (_) {}
                    storageHealth = { ok: true, error: null, lastIncident: null };
                } else {
                    allRestored = false;
                    recoveryError = 'Post-recovery file verification failed';
                }
            }

            if (!allRestored) {
                console.error(`[Startup Recovery] Recovery incomplete: ${recoveryError}. Retaining ${TRANSACTION_FILE} for retry.`);
                storageHealth = {
                    ok: false,
                    error: `Startup recovery incomplete: ${recoveryError}`,
                    lastIncident: new Date().toISOString()
                };
            }
        } else if (tx && tx.status === 'committed') {
            try { fs.unlinkSync(TRANSACTION_FILE); } catch (_) {}
        }
    }
} catch (txErr) {
    console.error('[Startup Recovery] Transaction recovery error:', txErr.message);
    storageHealth = {
        ok: false,
        error: `Startup recovery exception: ${txErr.message}`,
        lastIncident: new Date().toISOString()
    };
}

// 2. Legacy Raw PHPSESSID/utk Key Migration to Safe Hashes
let accountsMigrated = false;
let messagesMigrated = false;
if (accountSessions && typeof accountSessions === 'object' && Object.keys(accountSessions).length > 0) {
    const keyMap = new Map(); // oldRawKey -> canonicalSafeKey
    for (const [key, acc] of Object.entries(accountSessions)) {
        if (!acc) continue;
        const canonical = extractStableAccountId(acc.cookies, acc.utk) || acc.accountKey;
        if (!canonical) continue;
        if (!Array.isArray(acc.legacyKeys)) acc.legacyKeys = [];

        // 1. If key in accountSessions is legacy, re-key to canonical and preserve old key in legacyKeys
        if (canonical !== key) {
            console.log(`[Startup Migration] Migrating legacy account key from "${key.slice(0, 10)}..." to "${canonical}"`);
            keyMap.set(key, canonical);
            acc.accountKey = canonical;
            if (!acc.legacyKeys.includes(key)) acc.legacyKeys.push(key);
            accountSessions[canonical] = acc;
            delete accountSessions[key];
            accountsMigrated = true;
        }

        // 2. Register all known legacyKeys so message migration remains repeatable across restarts
        for (const lk of acc.legacyKeys) {
            if (lk && lk !== canonical) {
                keyMap.set(lk, canonical);
            }
        }

        // 3. Register raw PHPSESSID if present in cookies
        const phpMatch = (acc.cookies || '').match(/PHPSESSID=([^;]+)/i);
        if (phpMatch && phpMatch[1]) {
            const rawPhp = phpMatch[1].trim();
            if (rawPhp && rawPhp !== canonical) {
                keyMap.set(rawPhp, canonical);
                if (!acc.legacyKeys.includes(rawPhp)) {
                    acc.legacyKeys.push(rawPhp);
                    accountsMigrated = true;
                }
            }
        }

        // 4. Register raw utk / utks / historicUtks
        if (acc.utk && acc.utk !== canonical) {
            keyMap.set(acc.utk, canonical);
        }
        if (Array.isArray(acc.utks)) {
            for (const u of acc.utks) {
                if (u && u !== canonical) keyMap.set(u, canonical);
            }
        }
        if (Array.isArray(acc.historicUtks)) {
            for (const hu of acc.historicUtks) {
                if (hu && hu !== canonical) keyMap.set(hu, canonical);
            }
        }
    }

    if (keyMap.size > 0 && Array.isArray(messages)) {
        messages.forEach(m => {
            if (m && m.owner && keyMap.has(m.owner)) {
                const targetOwner = keyMap.get(m.owner);
                if (m.owner !== targetOwner) {
                    m.owner = targetOwner;
                    messagesMigrated = true;
                }
            }
        });
    }

    let migrationSaveFailed = false;
    if (accountsMigrated) {
        if (!saveJson(ACCOUNTS_FILE, accountSessions)) {
            migrationSaveFailed = true;
            console.error('[Startup Migration] Failed to persist migrated accounts.json');
        }
    }
    if (messagesMigrated) {
        if (!saveJson(MESSAGES_FILE, messages)) {
            migrationSaveFailed = true;
            console.error('[Startup Migration] Failed to persist migrated messages.json');
        }
    }
    if (migrationSaveFailed) {
        storageHealth = {
            ok: false,
            error: 'Startup migration persistence failure: unable to fully write accounts or messages to disk',
            lastIncident: new Date().toISOString()
        };
    }
}


// 3. Startup reconciliation: Ensure bidirectional sync between sessionData and accountSessions
function ensureSessionDataInAccounts() {
    if (sessionData && (sessionData.utk || sessionData.cookies)) {
        const stableId = extractStableAccountId(sessionData.cookies, sessionData.utk) || 'default_owner';
        if (!accountSessions[stableId]) {
            accountSessions[stableId] = {
                accountKey: stableId,
                cookies: sessionData.cookies || '',
                utk: sessionData.utk || '',
                userAgent: sessionData.userAgent || '',
                utks: [sessionData.utk].filter(Boolean),
                historicUtks: [sessionData.utk].filter(Boolean),
                lastUpdated: sessionData.lastUpdated || new Date().toISOString()
            };
            saveJson(ACCOUNTS_FILE, accountSessions);
        } else {
            if (sessionData.utk && Array.isArray(accountSessions[stableId].utks) && !accountSessions[stableId].utks.includes(sessionData.utk)) {
                accountSessions[stableId].utks.push(sessionData.utk);
                saveJson(ACCOUNTS_FILE, accountSessions);
            }
        }
    }
}

if (accountSessions && typeof accountSessions === 'object' && Object.keys(accountSessions).length > 0) {
    const currentSessionKey = extractStableAccountId(sessionData.cookies, sessionData.utk);
    const activeAccounts = Object.values(accountSessions).filter(acc => acc && !acc.revoked && (acc.cookies || acc.utk));

    // If sessionData points to an uncommitted/orphaned account not in accountSessions:
    if (currentSessionKey && (!accountSessions[currentSessionKey] || accountSessions[currentSessionKey].revoked)) {
        console.warn(`[Startup] Detected orphaned session for ${currentSessionKey}. Reconciling with accounts.json...`);
        if (activeAccounts.length > 0) {
            sessionData.cookies = activeAccounts[0].cookies || '';
            sessionData.utk = activeAccounts[0].utk || '';
            sessionData.userAgent = activeAccounts[0].userAgent || '';
            sessionData.lastUpdated = activeAccounts[0].lastUpdated || new Date().toISOString();
        } else {
            sessionData.cookies = '';
            sessionData.utk = '';
        }
        saveJson(SESSION_FILE, sessionData);
    } else if ((!sessionData.cookies || !sessionData.utk) && activeAccounts.length > 0) {
        if (!sessionData.cookies && activeAccounts[0].cookies) sessionData.cookies = activeAccounts[0].cookies;
        if (!sessionData.utk && activeAccounts[0].utk) sessionData.utk = activeAccounts[0].utk;
        if (!sessionData.userAgent && activeAccounts[0].userAgent) sessionData.userAgent = activeAccounts[0].userAgent;
    }
} else {
    ensureSessionDataInAccounts();
}

function generateRecoveryKey() {
    try {
        if (typeof crypto !== 'undefined' && crypto.randomUUID) {
            return 'rec_' + crypto.randomUUID().replace(/-/g, '');
        }
        const nodeCrypto = require('crypto');
        return 'rec_' + nodeCrypto.randomBytes(16).toString('hex');
    } catch (e) {
        return 'rec_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
    }
}

function getAdminRecoveryKey() {
    return (process.env.GHOST_ADMIN_KEY || process.env.GHOST_RECOVERY_KEY || '').trim();
}

function hasRegisteredAccounts() {
    for (const acc of Object.values(accountSessions)) {
        if (acc && !acc.revoked && ((acc.utks && acc.utks.length > 0) || acc.utk || acc.cookies)) return true;
    }
    if (sessionData && (sessionData.utk || sessionData.cookies)) return true;
    return false;
}

function getOwnerId(req) {
    ensureSessionDataInAccounts();
    const activeEntries = Object.entries(accountSessions).filter(([k, acc]) => acc && !acc.revoked);
    const activeKeys = activeEntries.map(([k]) => k);

    // 1. Authenticated client context via request headers, body, or query
    if (req) {
        const headers = req.headers || {};
        const clientToken = headers['x-ghost-token'] || (req.body && (req.body.sessionToken || req.body.token)) || (req.query && (req.query.sessionToken || req.query.token));
        if (clientToken && typeof clientToken === 'string') {
            const cleanToken = clientToken.trim();
            for (const [key, acc] of activeEntries) {
                if (acc.utk === cleanToken || (Array.isArray(acc.utks) && acc.utks.includes(cleanToken))) {
                    return key;
                }
            }
            if (sessionData && sessionData.utk && sessionData.utk === cleanToken) {
                const stableId = extractStableAccountId(sessionData.cookies, sessionData.utk) || 'default_owner';
                return stableId;
            }

            // Single Account / Single Tenant fallback: If only 1 account exists, auto-associate the token
            if (activeKeys.length === 1) {
                const singleKey = activeKeys[0];
                const singleAcc = accountSessions[singleKey];
                if (singleAcc && cleanToken && !cleanToken.startsWith('unauthorized_') && cleanToken !== '59b7cd4aa213bda6918f3a74b736182b') {
                    if (!singleAcc.utks) singleAcc.utks = [singleAcc.utk].filter(Boolean);
                    if (!singleAcc.utks.includes(cleanToken)) {
                        singleAcc.utks.push(cleanToken);
                        if (!singleAcc.historicUtks) singleAcc.historicUtks = [];
                        if (!singleAcc.historicUtks.includes(cleanToken)) singleAcc.historicUtks.push(cleanToken);
                        saveJson(ACCOUNTS_FILE, accountSessions);
                    }
                }
                return singleKey;
            }

            return 'unauthorized_token_' + cleanToken.replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 16);
        }

        // x-ghost-session header without secret token is explicitly rejected to prevent forgery
        if (headers['x-ghost-session']) {
            return 'unauthorized_forged_session';
        }

        // If no token is passed and exactly 1 active account exists, return it!
        if (activeKeys.length === 1) {
            return activeKeys[0];
        }

        if (hasRegisteredAccounts()) {
            return 'unauthorized_missing_token';
        }

        return 'default_owner';
    }

    // 2. Default to active sessionData on the server ONLY for internal background operations
    if (sessionData) {
        const stableId = extractStableAccountId(sessionData.cookies, sessionData.utk);
        if (stableId) return stableId;
    }

    if (activeKeys.length === 1) {
        return activeKeys[0];
    }

    return 'default_owner';
}


function isDuplicateMessage(existing, incoming) {
    if (!existing || !incoming) return false;
    if (existing.id && incoming.id && String(existing.id) === String(incoming.id)) {
        if (existing.owner && incoming.owner && existing.owner !== incoming.owner) {
            return false;
        }
        if (existing.peerId && incoming.peerId && String(existing.peerId) !== String(incoming.peerId)) {
            return false;
        }
        if (existing.type && incoming.type && existing.type !== incoming.type) {
            return false;
        }
        return true;
    }
    return false;
}

// Clean text helpers
function stripHtml(html) {
    if (!html) return '';
    return html.replace(/<[^>]*>?/gm, '').trim();
}

function parseIncomingMessage(data, targetOwner = null) {
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
        let bubbleHtml = contentMatch && contentMatch[1] ? contentMatch[1].trim() : msgHtml;
        bubbleHtml = bubbleHtml.replace(/<span[^>]*class="[^"]*(?:username|user_name|target_name)[^"]*"[^>]*>[\s\S]*?<\/span>/gi, '').trim();
        bubbleHtml = bubbleHtml.replace(/<img[^>]*class="[^"]*(?:avatar_private|private_avatar)[^"]*"[^>]*>/gi, '').trim();
        msgHtml = bubbleHtml;
        msgText = (data.message || data.msg || data.content) ? String(data.message || data.msg || data.content).trim() : stripHtml(msgHtml);
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
        type: (data.own || data.is_own || data.hunter || (data.html && /hunter_private/i.test(String(data.html)))) ? 'sent' : 'received',
        synced: false,
        owner: targetOwner || getOwnerId(null)
    };
}

// ==========================================
// Multi-Account Background Socket Pool
// ==========================================
const accountSockets = new Map(); // accKey -> { socket, connected, cookies, utk }

function disconnectAccountSocket(accKey) {
    if (accountSockets.has(accKey)) {
        const item = accountSockets.get(accKey);
        try {
            if (item.socket) {
                item.socket.removeAllListeners();
                item.socket.disconnect();
            }
        } catch (e) {}
        accountSockets.delete(accKey);
        addLog(`[SocketPool] Disconnected background socket for account: ${accKey}`);
    }
}

const SOCKET_CONNECT_TIMEOUT = 20000;

function connectAccountSocket(accKey, cookies, utk, userAgent) {
    if (!cookies || !utk) return null;

    if (accountSockets.has(accKey)) {
        const existing = accountSockets.get(accKey);
        const credentialsMatch = existing.cookies === cookies && existing.utk === utk;

        if (credentialsMatch && existing.socket) {
            // 1. If live and connected, return existing live socket
            if (existing.socket.connected && existing.connected) {
                existing.connecting = false;
                return existing.socket;
            }

            // 2. If a connection attempt is actively in progress within timeout, do not tear down prematurely
            const elapsed = existing.connectingStartedAt ? (Date.now() - existing.connectingStartedAt) : 0;
            if (existing.connecting && elapsed < SOCKET_CONNECT_TIMEOUT) {
                return existing.socket;
            }

            // 3. Initiate or retry connect() on existing socket
            if (typeof existing.socket.connect === 'function') {
                try {
                    existing.connecting = true;
                    existing.connectingStartedAt = Date.now();
                    existing.socket.connect();
                    if (existing.socket.connected) {
                        existing.connected = true;
                        existing.connecting = false;
                        existing.lastError = null;
                    }
                    return existing.socket;
                } catch (e) {
                    existing.connecting = false;
                }
            }
        }

        disconnectAccountSocket(accKey);
    }

    const headers = {
        'Origin': SITE_URL,
        'Referer': SITE_URL + '/',
        'User-Agent': userAgent || (sessionData && sessionData.userAgent) || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'Cookie': cookies
    };

    console.log(`[SocketPool] Connecting socket for account ${accKey} to ${SITE_URL}${SOCKET_PATH}...`);
    try {
        const sock = io(SITE_URL, {
            path: SOCKET_PATH,
            transports: ['websocket'],
            extraHeaders: headers,
            reconnection: true,
            reconnectionDelay: 1000,
            reconnectionDelayMax: 10000,
            autoConnect: true,
            timeout: 20000
        });

        const entry = {
            socket: sock,
            connected: Boolean(sock.connected),
            connecting: !sock.connected,
            connectingStartedAt: sock.connected ? null : Date.now(),
            cookies: cookies,
            utk: utk,
            lastConnectedTime: sock.connected ? new Date().toISOString() : null,
            lastError: null,
            lastMessageTime: null
        };
        accountSockets.set(accKey, entry);
        chatSocket = sock; // Maintain backward compatibility for single-socket callers and tests
        if (sock.connected) {
            socketConnected = true;
            lastConnectedTime = entry.lastConnectedTime;
        }

        sock.on('connect', () => {
            entry.connected = true;
            entry.connecting = false;
            entry.connectingStartedAt = null;
            entry.lastConnectedTime = new Date().toISOString();
            entry.lastError = null;
            entry.authFailures = 0;
            entry.authExpired = false;
            socketConnected = true;
            lastConnectedTime = entry.lastConnectedTime;
            lastError = null;
            addLog(`[Socket:${accKey}] Connected successfully! Socket ID: ${sock.id}`);
        });

        sock.on('disconnect', (reason) => {
            entry.connected = false;
            entry.connecting = false;
            socketConnected = Array.from(accountSockets.values()).some(e => e.connected);
            addLog(`[Socket:${accKey}] Disconnected (reason: ${reason})`);
        });

        sock.on('connect_error', (err) => {
            entry.connected = false;
            entry.connecting = false;
            entry.lastError = err ? err.message : 'Unknown connect error';
            socketConnected = Array.from(accountSockets.values()).some(e => e.connected);
            lastError = entry.lastError;
            addLog(`[Socket:${accKey}] Connection error: ${entry.lastError}`);

            if (err && (err.message === 'unauthorized' || String(err.message || '').includes('unauthorized'))) {
                entry.authFailures = (entry.authFailures || 0) + 1;
                if (entry.authFailures >= 2) {
                    entry.authExpired = true;
                    if (sock && typeof sock.disconnect === 'function') sock.disconnect();
                    addLog(`[Socket:${accKey}] Token expired (unauthorized). Auto-pausing reconnection watchdog until updated session is pushed.`);
                }
            }
        });

        sock.on('error', (err) => {
            addLog(`[Socket:${accKey}] Generic socket error: ${err}`);
        });

        if (sock.io) {
            sock.io.on('error', (err) => {
                addLog(`[Socket Manager:${accKey}] Engine error: ${err}`);
            });
            sock.io.on('reconnect_error', (err) => {
                addLog(`[Socket Manager:${accKey}] Reconnect error: ${err}`);
            });
        }

        sock.onAny((event, ...args) => {
            if (event !== 'ping' && event !== 'pong') {
                if (event === 'private-msg' || event.includes('msg')) {
                    addLog(`[Socket Event:${accKey}] ${event} (payload redacted for privacy)`);
                } else {
                    addLog(`[Socket Event:${accKey}] ${event}`);
                }
            }
        });

        sock.on('private-msg', (data) => {
            try {
                entry.lastMessageTime = new Date().toISOString();
                addLog(`[Socket:${accKey}] Incoming private-msg received!`);
                const parsed = parseIncomingMessage(data, accKey);
                if (!parsed) return;

                if (isPeerTombstoned(accKey, parsed.peerId, parsed.timestamp)) {
                    addLog(`[Socket:${accKey}] Dropping tombstoned message for peer ${parsed.peerId}`);
                    return;
                }

                // Check for duplicate using composite logic
                const exists = messages.some(m => isDuplicateMessage(m, parsed));
                if (!exists) {
                    parsed.seq = nextOwnerSeq(accKey);
                    const candidate = [...messages, parsed];
                    const trimmed = applyRetentionPolicy(candidate);
                    const saved = saveJson(MESSAGES_FILE, trimmed);
                    if (saved) {
                        messages = trimmed;
                        recordIncomingToPendingTransaction(parsed);
                        addLog(`[Ghost Cloud] Captured message for account ${accKey} (id: ${parsed.id}, seq: ${parsed.seq})`);
                    } else {
                        console.error('[Socket] Failed to persist captured message to disk');
                    }
                }
            } catch (err) {
                console.error(`[Socket:${accKey}] Error processing private-msg:`, err);
            }
        });

        return sock;
    } catch (e) {
        lastError = e.message;
        addLog(`[Socket:${accKey}] Exception initializing socket: ${e.message}`);
        return null;
    }
}

function initChatSocket() {
    // 1. Remove sockets for revoked or non-existent accounts
    for (const [k, entry] of accountSockets.entries()) {
        const acc = accountSessions[k];
        if (!acc || acc.revoked || !acc.cookies || !acc.utk) {
            disconnectAccountSocket(k);
        }
    }

    // 2. Connect sockets for all active registered accounts
    let connectedAny = false;
    for (const [accKey, acc] of Object.entries(accountSessions)) {
        if (acc && !acc.revoked && acc.cookies && acc.utk) {
            connectAccountSocket(accKey, acc.cookies, acc.utk, acc.userAgent);
            connectedAny = true;
        }
    }

    // 3. Fallback: if no registered accounts in accountSessions but sessionData has credentials
    if (!connectedAny && sessionData && sessionData.cookies && sessionData.utk) {
        const stableId = extractStableAccountId(sessionData.cookies, sessionData.utk) || 'default_owner';
        connectAccountSocket(stableId, sessionData.cookies, sessionData.utk, sessionData.userAgent);
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

async function pollSingleAccount(target) {
    if (!target.cookies && !target.utk) return;

    try {
        const headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': target.userAgent || (sessionData && sessionData.userAgent) || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'Origin': SITE_URL,
            'Referer': `${SITE_URL}/`
        };
        if (target.cookies) {
            headers['Cookie'] = target.cookies;
        }

        const bodyData = target.utk ? `token=${encodeURIComponent(target.utk)}` : '';

        // 1. Poll private_notify.php
        const notifyRes = await fetch(`${SITE_URL}/system/box/private_notify.php`, {
            method: 'POST',
            headers: headers,
            body: bodyData,
            signal: AbortSignal.timeout(10000)
        });

        // 1.1 Zero-Seen Keep-Alive Heartbeat (priv=0 ensures no message is marked read)
        if (target.utk) {
            try {
                fetch(`${SITE_URL}/system/chat_log.php`, {
                    method: 'POST',
                    headers: headers,
                    body: `fload=0&caction=0&taction=0&last=0&snum=0&preload=0&priv=0&lastp=0&pcount=0&room=1&notify=0&token=${encodeURIComponent(target.utk)}&r=${Date.now()}`,
                    signal: AbortSignal.timeout(10000)
                }).catch(() => {});
            } catch (err) {}
        }

        if (!notifyRes.ok) {
            lastPollStats.status = `HTTP_${notifyRes.status}`;
            return;
        }

        const notifyHtml = await notifyRes.text();
        lastPollStats.lastPollTime = new Date().toISOString();
        lastPollStats.totalCycles++;
        lastPollStats.status = 'ok';

        if (!notifyHtml || notifyHtml.trim().length === 0) return;

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
        for (const [peerId, info] of peersToCheck.entries()) {
            messages.forEach(m => {
                if (m.peerId === peerId && (!m.owner || m.owner === target.key)) {
                    if (info.name && m.name === ('مستخدم ' + peerId)) m.name = info.name;
                    if (info.avatar && m.avatar === 'default_images/avatar/default_avatar.png') m.avatar = info.avatar;
                }
            });

            if (info.unreadCount > 0) {
                lastPollStats.lastUnreadFound = info.unreadCount;
                console.log(`[Cloud Poller:${target.key}] 👻 Unread private notification from ${info.name} (${peerId}): ${info.unreadCount} unread message(s) [Zero-Seen active]`);

                const sockEntry = accountSockets.get(target.key);
                if (!sockEntry || !sockEntry.connected || !sockEntry.socket || !sockEntry.socket.connected) {
                    console.log(`[Cloud Poller:${target.key}] Socket disconnected for account ${target.key}. Reconnecting this account...`);
                    connectAccountSocket(target.key, target.cookies, target.utk, target.userAgent);
                }
            }
        }
    } catch (err) {
        lastPollStats.lastError = err.message;
        console.warn(`[Cloud Poller:${target.key}] Polling cycle error:`, err.message);
    }
}

async function pollArabicChatOnce() {
    const targets = [];
    for (const [k, acc] of Object.entries(accountSessions)) {
        if (acc && !acc.revoked && (acc.cookies || acc.utk)) {
            targets.push({ key: k, cookies: acc.cookies, utk: acc.utk, userAgent: acc.userAgent });
        }
    }
    if (targets.length === 0 && sessionData && (sessionData.utk || sessionData.cookies)) {
        targets.push({ key: 'default', cookies: sessionData.cookies, utk: sessionData.utk, userAgent: sessionData.userAgent });
    }

    if (targets.length === 0) {
        lastPollStats.status = 'no_credentials';
        return;
    }
    if (isPollingActive) return;
    isPollingActive = true;

    try {
        for (const target of targets) {
            await pollSingleAccount(target);
        }
    } finally {
        isPollingActive = false;
    }
}

function startPollingEngine() {
    if (pollIntervalTimer) clearInterval(pollIntervalTimer);
    console.log('[Cloud Poller] Starting 24/7 background polling engine (interval: 5s)...');
    setTimeout(pollArabicChatOnce, 1500);
    pollIntervalTimer = setInterval(pollArabicChatOnce, 5000);

    // 10-second Active Socket Watchdog (Per-Account Recovery)
    setInterval(() => {
        const hasSessionCreds = Boolean(sessionData && (sessionData.cookies || sessionData.utk));
        const hasActiveAccounts = Object.values(accountSessions).some(a => !a.revoked && a.cookies && a.utk);
        if (!hasSessionCreds && !hasActiveAccounts) {
            return; // Suspended: no active credentials or explicitly revoked
        }
        if (socketConnectingStartedAt && (Date.now() - socketConnectingStartedAt < 25000)) {
            return; // Allow active handshake up to 25s without premature termination
        }

        let hasActiveTarget = false;
        for (const [accKey, acc] of Object.entries(accountSessions)) {
            if (acc && !acc.revoked && acc.cookies && acc.utk) {
                const entry = accountSockets.get(accKey);
                if (entry && entry.authExpired) {
                    continue; // Skip reconnecting on expired/unauthorized account until updated
                }
                hasActiveTarget = true;
                const isConnecting = entry && entry.connecting && (Date.now() - (entry.connectingStartedAt || 0) < 20000);
                if (!isConnecting && (!entry || !entry.connected || !entry.socket || !entry.socket.connected)) {
                    console.log(`[Socket Watchdog] Socket dropped or idle for account ${accKey}. Reconnecting account ${accKey}...`);
                    connectAccountSocket(accKey, acc.cookies, acc.utk, acc.userAgent);
                }
            }
        }

        // Fallback for single sessionData if accountSessions has no active targets
        if (!hasActiveTarget && sessionData && sessionData.cookies && sessionData.utk) {
            const stableId = extractStableAccountId(sessionData.cookies, sessionData.utk) || 'default_owner';
            const entry = accountSockets.get(stableId);
            const isConnecting = entry && entry.connecting && (Date.now() - (entry.connectingStartedAt || 0) < 20000);
            if (!isConnecting && (!entry || !entry.connected || !entry.socket || !entry.socket.connected)) {
                console.log(`[Socket Watchdog] Fallback socket dropped or idle for ${stableId}. Reconnecting...`);
                connectAccountSocket(stableId, sessionData.cookies, sessionData.utk, sessionData.userAgent);
            }
        }
    }, 10000);
}

// Middleware
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

function checkAuth(req) {
    if (!GHOST_SECRET || GHOST_SECRET === 'ghost_secret_2026') return false;
    const key = (req.headers && req.headers['x-ghost-secret']) || (req.body && req.body.key) || (req.query && req.query.key);
    return Boolean(key && key === GHOST_SECRET && key !== 'ghost_secret_2026');
}

// Auth middleware for /api/*
function requireAuth(req, res, next) {
    if (!checkAuth(req)) {
        if (!GHOST_SECRET || GHOST_SECRET === 'ghost_secret_2026') {
            return res.status(401).json({ ok: false, error: 'Unauthorized: Server secret is not configured or uses insecure default' });
        }
        return res.status(401).json({ ok: false, error: 'Unauthorized: Invalid secret key' });
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
            <input type="text" id="token_input" class="login_input" placeholder="رمز الحساب الموثق (إلزامي / Account Token)">
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
        let ACCOUNT_TOKEN = localStorage.getItem('ghost_account_token') || '';
        let conversations = [];
        let activePeerId = null;

        const loginOverlay = document.getElementById('login_overlay');
        const secretInput = document.getElementById('secret_input');
        const tokenInput = document.getElementById('token_input');
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

        if (SECRET_KEY && ACCOUNT_TOKEN) {
            loginOverlay.style.display = 'none';
            initApp();
        } else {
            loginOverlay.style.display = 'flex';
        }

        loginBtn.onclick = function () {
            const key = secretInput.value.trim();
            if (!key) return alert("يرجى كتابة المفتاح السري");
            const token = tokenInput ? tokenInput.value.trim() : '';
            if (!token) return alert("يرجى إدخال رمز الحساب الموثق (Account Token)");
            SECRET_KEY = key;
            ACCOUNT_TOKEN = token;
            localStorage.setItem('ghost_secret_key', key);
            localStorage.setItem('ghost_account_token', token);
            loginOverlay.style.display = 'none';
            initApp();
        };

        logoutBtn.onclick = function () {
            if (confirm("تسجيل الخروج من لوحة الشبح؟")) {
                if (SECRET_KEY && ACCOUNT_TOKEN) {
                    fetch('/api/logout', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'x-ghost-secret': SECRET_KEY,
                            'x-ghost-token': ACCOUNT_TOKEN
                        }
                    }).catch(() => {});
                }
                localStorage.removeItem('ghost_secret_key');
                localStorage.removeItem('ghost_account_token');
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
            if (!SECRET_KEY || !ACCOUNT_TOKEN) return;
            const headers = {
                'x-ghost-secret': SECRET_KEY,
                'x-ghost-token': ACCOUNT_TOKEN
            };
            fetch('/api/conversations', {
                headers: headers
            })
                .then(r => {
                    if (r.status === 401) {
                        localStorage.removeItem('ghost_secret_key');
                        localStorage.removeItem('ghost_account_token');
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

        function escapeClientHtml(str) {
            if (!str) return '';
            return String(str).replace(/[&<>"']/g, function(c) {
                return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
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
                const rawAv = c.avatar || 'default_images/avatar/default_avatar.png';
                const avatar = rawAv.startsWith('http') ? rawAv : ('https://www.arabic.chat/' + rawAv.replace(/^\\/+/, ''));
                const safeAvatar = escapeClientHtml(avatar);
                const safePeerId = escapeClientHtml(c.peerId);
                const safeName = escapeClientHtml(c.name || ('مستخدم ' + c.peerId));
                const safeTime = escapeClientHtml(c.lastTime || '');
                const safeSnippet = escapeClientHtml(c.lastText || 'رسالة خاصة');
                return \`
                    <li class="contact_item \${isActive ? 'active' : ''}" data-peer="\${safePeerId}">
                        <img class="contact_avatar" src="\${safeAvatar}" onerror="this.src='https://www.arabic.chat/default_images/avatar/default_avatar.png'">
                        <div class="contact_info">
                            <div class="contact_header">
                                <span class="contact_name">\${safeName}</span>
                                <span class="contact_time">\${safeTime}</span>
                            </div>
                            <div class="contact_snippet">\${safeSnippet}</div>
                        </div>
                    </li>
                \`;
            }).join('');
        }

        contactsList.addEventListener('click', function (e) {
            const item = e.target.closest('li.contact_item');
            if (item && item.getAttribute('data-peer')) {
                openChat(item.getAttribute('data-peer'));
            }
        });

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

        function sanitizeClientHtml(htmlStr) {
            if (!htmlStr) return '';
            // If DOMParser is available (browser environment)
            if (typeof DOMParser !== 'undefined') {
                try {
                    const parser = new DOMParser();
                    const doc = parser.parseFromString(String(htmlStr), 'text/html');
                    const ALLOWED_TAGS = new Set(['div', 'span', 'p', 'b', 'strong', 'i', 'em', 'u', 's', 'strike', 'br', 'hr', 'img', 'audio', 'video', 'source', 'a']);
                    const ALLOWED_ATTRS = {
                        'img': new Set(['src', 'alt', 'class', 'style', 'width', 'height', 'loading']),
                        'audio': new Set(['src', 'controls', 'class', 'style', 'preload']),
                        'video': new Set(['src', 'controls', 'class', 'style', 'preload', 'width', 'height']),
                        'source': new Set(['src', 'type']),
                        'a': new Set(['href', 'target', 'rel', 'class', 'style']),
                        '*': new Set(['class', 'style'])
                    };

                    const elements = doc.body.querySelectorAll('*');
                    elements.forEach(function (el) {
                        const tag = el.tagName.toLowerCase();
                        if (!ALLOWED_TAGS.has(tag)) {
                            el.remove();
                            return;
                        }
                        Array.from(el.attributes).forEach(function (attr) {
                            const name = attr.name.toLowerCase();
                            if (name.startsWith('on') || (!ALLOWED_ATTRS['*'].has(name) && !(ALLOWED_ATTRS[tag] && ALLOWED_ATTRS[tag].has(name)))) {
                                el.removeAttribute(attr.name);
                                return;
                            }
                            if (name === 'href' || name === 'src') {
                                const raw = attr.value.replace(/[\\x00-\\x20\\s]+/g, '').toLowerCase();
                                if (name === 'href') {
                                    const isSafe = !raw.includes(':') || raw.startsWith('http://') || raw.startsWith('https://') || raw.startsWith('mailto:') || raw.startsWith('tel:') || raw.startsWith('#') || raw.startsWith('/') || raw.startsWith('./');
                                    if (!isSafe) el.removeAttribute(attr.name);
                                } else if (name === 'src') {
                                    const isSafe = !raw.includes(':') || raw.startsWith('http://') || raw.startsWith('https://') || raw.startsWith('blob:') || raw.startsWith('data:image/') || raw.startsWith('/') || raw.startsWith('./');
                                    if (!isSafe) el.removeAttribute(attr.name);
                                }
                            }
                        });
                    });
                    return doc.body.innerHTML;
                } catch (e) {}
            }

            // Robust String Fallback (decodes HTML entities and validates schemes)
            let cleaned = String(htmlStr)
                .replace(/<(script|iframe|object|embed|svg|link|style|meta)[\\s\\S]*?<\\/\\1>/gi, '')
                .replace(/<(script|iframe|object|embed|svg|link|style|meta)[^>]*\\/?>/gi, '')
                .replace(/\\s*on[a-zA-Z]+\\s*=\\s*("[^"]*"|'[^']*'|[^\\s>]+)/gi, '');

            cleaned = cleaned.replace(/(href|src)\\s*=\\s*("([^"]*)"|'([^']*)'|([^\\s>]+))/gi, function (match, attr, fullVal, v1, v2, v3) {
                const rawVal = v1 || v2 || v3 || '';
                const decoded = rawVal.replace(/&#(x[0-9a-f]+|[0-9]+);?/gi, function (_, code) {
                    const n = code.startsWith('x') || code.startsWith('X') ? parseInt(code.slice(1), 16) : parseInt(code, 10);
                    return String.fromCharCode(n);
                }).replace(/&colon;/gi, ':').replace(/[\\x00-\\x20\\s]+/g, '').toLowerCase();

                if (attr.toLowerCase() === 'href') {
                    const isSafe = !decoded.includes(':') || decoded.startsWith('http://') || decoded.startsWith('https://') || decoded.startsWith('mailto:') || decoded.startsWith('tel:') || decoded.startsWith('#') || decoded.startsWith('/') || decoded.startsWith('./');
                    return isSafe ? \`\${attr}="\${rawVal}"\` : \`\${attr}="#"\`;
                } else {
                    const isSafe = !decoded.includes(':') || decoded.startsWith('http://') || decoded.startsWith('https://') || decoded.startsWith('blob:') || decoded.startsWith('data:image/') || decoded.startsWith('/') || decoded.startsWith('./');
                    return isSafe ? \`\${attr}="\${rawVal}"\` : '';
                }
            });
            return cleaned;
        }

        function renderActiveMessages(scrollBottom = false) {
            if (!activePeerId) return;
            const target = conversations.find(c => c.peerId === activePeerId);
            if (!target || !target.messages || target.messages.length === 0) {
                messagesArea.innerHTML = '<div class="empty_view"><i class="fa fa-comment-slash"></i>لا توجد رسائل سابقة في هذه المحادثة</div>';
                return;
            }

            messagesArea.innerHTML = target.messages.map(m => {
                const isSent = m.type === 'sent';
                let content = sanitizeClientHtml(m.html || m.text || '');
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
    const isAuthed = checkAuth(req);
    const isAnySocketConnected = accountSockets.size > 0 ? Array.from(accountSockets.values()).some(e => e.connected) : socketConnected;

    // Public health check response: concise, zero-sensitive metadata
    if (!isAuthed) {
        return res.json({
            ok: true,
            status: 'online',
            service: 'ghost-cloud-relay'
        });
    }

    const activeKeys = Object.keys(accountSessions).filter(k => accountSessions[k] && !accountSessions[k].revoked);
    const reqOwner = getOwnerId(req);
    let accountConnected = undefined;
    let accountLastError = undefined;
    let accountLastConnectedTime = undefined;
    let accountLastMessageTime = undefined;

    const targetKey = (reqOwner && !reqOwner.startsWith('unauthorized_') && reqOwner !== 'default_owner') 
        ? reqOwner 
        : (activeKeys.length === 1 ? activeKeys[0] : null);

    if (targetKey) {
        const entry = accountSockets.get(targetKey);
        if (entry) {
            accountConnected = !!entry.connected;
            accountLastError = entry.lastError;
            accountLastConnectedTime = entry.lastConnectedTime;
            accountLastMessageTime = entry.lastMessageTime;
        } else {
            accountConnected = false;
            accountLastError = 'Account socket not connected';
        }
    }

    const targetAcc = targetKey ? accountSessions[targetKey] : null;
    const effCookies = (targetAcc && targetAcc.cookies) || sessionData.cookies || '';
    const effUtk = (targetAcc && targetAcc.utk) || sessionData.utk || '';
    const hasPhp = effCookies.includes('PHPSESSID');
    const cookieKeys = effCookies ? effCookies.split(';').map(c => c.trim().split('=')[0]).filter(Boolean) : [];
    const hasAuthCookies = Boolean(
        effCookies && (
            hasPhp ||
            effCookies.includes('bc_auth') ||
            effCookies.includes('chat-session') ||
            cookieKeys.length > 0
        )
    );

    const accountsSummary = {};
    for (const [k, acc] of Object.entries(accountSessions)) {
        if (!acc.revoked) {
            const entry = accountSockets.get(k);
            accountsSummary[k] = {
                connected: entry ? !!entry.connected : false,
                lastConnectedTime: entry ? entry.lastConnectedTime : null,
                lastError: entry ? entry.lastError : null,
                lastMessageTime: entry ? entry.lastMessageTime : null
            };
        }
    }

    const relevantMessages = targetKey ? messages.filter(m => m.owner === targetKey) : messages;

    res.json({
        ok: true,
        socketConnected: isAnySocketConnected,
        siteUrl: SITE_URL,
        hasSession: !!(effUtk || effCookies),
        sessionUtkPresent: !!effUtk,
        sessionCookiesPresent: !!effCookies,
        hasPhpsessid: hasPhp || hasAuthCookies,
        hasSessionCookies: hasAuthCookies,
        cookieKeys: cookieKeys,
        sessionLastUpdated: (targetAcc && targetAcc.lastUpdated) || sessionData.lastUpdated,
        lastConnectedTime: accountLastConnectedTime || lastConnectedTime,
        lastError: accountLastError || lastError,
        accountConnected: accountConnected !== undefined ? accountConnected : isAnySocketConnected,
        accountLastError: accountLastError || lastError,
        accountLastConnectedTime: accountLastConnectedTime || lastConnectedTime,
        accountLastMessageTime: accountLastMessageTime,
        accounts: accountsSummary,
        retentionPolicy: 'fifo',
        maxRetentionMessages: MAX_MESSAGES,
        currentRetentionCount: messages.length,
        pollerActive: true,
        storageHealth: storageHealth,
        lastPollTime: lastPollStats.lastPollTime,
        lastPollStatus: lastPollStats.status,
        lastPollCycles: lastPollStats.totalCycles,
        totalMessages: relevantMessages.length,
        unsyncedCount: relevantMessages.filter(m => !m.synced).length
    });
});

// 2.1 Live Logs API (For inspection & forensics)
app.get('/api/logs', requireAuth, (req, res) => {
    res.json({
        ok: true,
        count: serverLogs.length,
        logs: serverLogs
    });
});

// 3. Sync Messages (Multi-Device Safe: PC, Mobile, PWA with Monotonic seq_id & Device Cursors)
app.get('/api/sync', requireAuth, (req, res) => {
    const currentOwner = getOwnerId(req);
    if (!currentOwner || currentOwner.startsWith('unauthorized_')) {
        return res.status(401).json({
            ok: false,
            count: 0,
            messages: [],
            error: 'Unauthorized: valid account token is required',
            serverTime: Date.now()
        });
    }
    const markAsSynced = req.query.mark === 'true'; // Legacy fallback
    const getAll = req.query.all !== 'false'; // Default to TRUE so all devices get full sync
    const since = parseInt(req.query.since || '0', 10);
    const deviceId = req.query.device_id ? String(req.query.device_id).slice(0, 128) : null;
    const hasAfterSeq = req.query.after_seq !== undefined || req.query.since_seq !== undefined;
    const rawAfterSeq = req.query.after_seq !== undefined ? req.query.after_seq : req.query.since_seq;
    const afterSeq = hasAfterSeq ? parseInt(rawAfterSeq, 10) : null;

    const ownerMessages = messages.filter(m => Boolean(m.owner) && m.owner === currentOwner);
    const currentSeq = ownerSeqCounters[currentOwner] || 0;

    let resultMsgs = [];
    let snapshotRequired = false;

    if (afterSeq !== null && !isNaN(afterSeq)) {
        if (ownerMessages.length > 0) {
            const minSeq = Math.min(...ownerMessages.map(m => (typeof m.seq === 'number' ? m.seq : 1)));
            if (afterSeq > 0 && afterSeq < minSeq - 1) {
                // Device lagged behind pruned retention horizon -> require snapshot
                snapshotRequired = true;
                resultMsgs = [...ownerMessages];
            } else {
                resultMsgs = ownerMessages.filter(m => (m.seq || 0) > afterSeq);
            }
        } else {
            resultMsgs = [];
        }
    } else if (getAll) {
        resultMsgs = since > 0 ? ownerMessages.filter(m => m.timestamp > since) : [...ownerMessages];
    } else {
        resultMsgs = ownerMessages.filter(m => !m.synced && (since === 0 || m.timestamp > since));
    }

    if (markAsSynced && resultMsgs.length > 0) {
        const candidate = messages.map(m => {
            const shouldMark = resultMsgs.some(rm => isDuplicateMessage(m, rm));
            return shouldMark ? { ...m, synced: true } : m;
        });
        const saved = saveJson(MESSAGES_FILE, candidate);
        if (!saved) {
            return res.status(500).json({ ok: false, error: 'Persistence failure: unable to update sync status' });
        }
        messages = candidate;
    }

    // Note: GET /api/sync is strictly read-only and idempotent.
    // Device cursor is only advanced via explicit POST /api/sync/ack after client confirms local persistence.

    res.json({
        ok: true,
        count: resultMsgs.length,
        messages: resultMsgs,
        epoch: SERVER_EPOCH,
        current_seq: currentSeq,
        snapshot_required: snapshotRequired,
        serverTime: Date.now()
    });
});

// 3.1 Per-Device Cursor Acknowledgement API (Zero-Seen Safe)
app.post('/api/sync/ack', requireAuth, (req, res) => {
    const currentOwner = getOwnerId(req);
    if (!currentOwner || currentOwner.startsWith('unauthorized_')) {
        return res.status(401).json({ ok: false, error: 'Unauthorized: valid account token is required' });
    }

    const { device_id, ack_seq, epoch } = req.body || {};
    if (!device_id) {
        return res.status(400).json({ ok: false, error: 'Missing device_id' });
    }

    const safeDeviceId = String(device_id).slice(0, 128);
    const parsedAckSeq = parseInt(ack_seq, 10);
    if (isNaN(parsedAckSeq) || parsedAckSeq < 0) {
        return res.status(400).json({ ok: false, error: 'Invalid ack_seq' });
    }

    const cursorKey = `${currentOwner}:${safeDeviceId}`;
    const prevCursor = deviceCursors[cursorKey] || {};
    const epochMismatch = Boolean(epoch && Number(epoch) !== Number(SERVER_EPOCH));

    if (parsedAckSeq >= (prevCursor.ack_seq || 0) || epochMismatch) {
        deviceCursors[cursorKey] = {
            ack_seq: parsedAckSeq,
            last_sync: Date.now(),
            epoch: SERVER_EPOCH
        };
        saveJson(DEVICE_CURSORS_FILE, deviceCursors);
    }

    res.json({
        ok: true,
        device_id: safeDeviceId,
        ack_seq: parsedAckSeq,
        epoch: SERVER_EPOCH,
        epoch_mismatch: epochMismatch,
        current_seq: ownerSeqCounters[currentOwner] || 0
    });
});

// 4. Record Sent Message from PC (Two-Way Sync)
app.post('/api/messages/sent', requireAuth, (req, res) => {
    const { peerId, name, message } = req.body;
    if (!peerId || !message) {
        return res.status(400).json({ ok: false, error: 'Missing peerId or message' });
    }

    const safePeerId = String(peerId).replace(/["'<>]/g, '').slice(0, 64);
    const currentOwner = getOwnerId(req);
    if (!currentOwner || currentOwner.startsWith('unauthorized_')) {
        return res.status(401).json({ ok: false, error: 'Unauthorized: account token is required or invalid' });
    }
    const resolvedOwner = currentOwner;

    const sentItem = {
        id: message.id || ('msg_sent_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5)),
        peerId: safePeerId,
        name: name || ('مستخدم ' + safePeerId),
        avatar: message.avatar || 'default_images/avatar/default_avatar.png',
        text: message.text || stripHtml(message.html || ''),
        html: message.html || message.text || '',
        time: message.time || new Date().toLocaleTimeString('ar-EG', { hour: '2-digit', minute: '2-digit' }),
        timestamp: message.timestamp || Date.now(),
        type: 'sent',
        synced: true,
        owner: resolvedOwner
    };

    if (isPeerTombstoned(resolvedOwner, safePeerId, sentItem.timestamp)) {
        return res.json({ ok: true, data: sentItem, tombstoned: true });
    }

    const exists = messages.some(m => isDuplicateMessage(m, sentItem));
    if (!exists) {
        sentItem.seq = nextOwnerSeq(resolvedOwner);
        const candidate = [...messages, sentItem];
        const trimmed = applyRetentionPolicy(candidate);
        const saved = saveJson(MESSAGES_FILE, trimmed);
        if (!saved) {
            return res.status(500).json({ ok: false, error: 'Persistence failure: unable to write messages file' });
        }
        messages = trimmed;
        recordIncomingToPendingTransaction(sentItem);
        console.log(`[Ghost Cloud] Two-Way Sync: Recorded sent message (id: ${sentItem.id}, seq: ${sentItem.seq})`);
    }

    res.json({ ok: true, data: sentItem });
});

// 4.1 Record Incoming Message from Extension (Two-Way Sync)
app.post('/api/messages/incoming', requireAuth, (req, res) => {
    const { peerId, name, message } = req.body;
    if (!peerId || !message) {
        return res.status(400).json({ ok: false, error: 'Missing peerId or message' });
    }

    const safePeerId = String(peerId).replace(/["'<>]/g, '').slice(0, 64);
    const currentOwner = getOwnerId(req);
    if (!currentOwner || currentOwner.startsWith('unauthorized_')) {
        return res.status(401).json({ ok: false, error: 'Unauthorized: account token is required or invalid' });
    }

    const incomingItem = {
        id: message.id || ('msg_in_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5)),
        peerId: safePeerId,
        name: name || ('مستخدم ' + safePeerId),
        avatar: message.avatar || 'default_images/avatar/default_avatar.png',
        text: message.text || stripHtml(message.html || ''),
        html: message.html || message.text || '',
        time: message.time || new Date().toLocaleTimeString('ar-EG', { hour: '2-digit', minute: '2-digit' }),
        timestamp: message.timestamp || Date.now(),
        type: 'received',
        synced: true, // Already handled by the reporting extension
        owner: currentOwner
    };

    if (isPeerTombstoned(currentOwner, safePeerId, incomingItem.timestamp)) {
        return res.json({ ok: true, data: incomingItem, tombstoned: true });
    }

    const exists = messages.some(m => isDuplicateMessage(m, incomingItem));
    if (!exists) {
        incomingItem.seq = nextOwnerSeq(currentOwner);
        const candidate = [...messages, incomingItem];
        const trimmed = applyRetentionPolicy(candidate);
        const saved = saveJson(MESSAGES_FILE, trimmed);
        if (!saved) {
            return res.status(500).json({ ok: false, error: 'Persistence failure: unable to write messages file' });
        }
        messages = trimmed;
        recordIncomingToPendingTransaction(incomingItem);
        console.log(`[Ghost Cloud] Two-Way Sync: Recorded incoming message (id: ${incomingItem.id}, seq: ${incomingItem.seq})`);
    }

    res.json({ ok: true, data: incomingItem });
});

// 4.1 Delete Peer Messages API (For Tombstone / Contact Deletion Sync)
app.post('/api/messages/delete-peer', requireAuth, (req, res) => {
    const { peerId, beforeTimestamp } = req.body;
    if (!peerId) {
        return res.status(400).json({ ok: false, error: 'Missing peerId parameter' });
    }
    const cutoff = Number(beforeTimestamp);
    if (!cutoff || isNaN(cutoff) || cutoff <= 0) {
        return res.status(400).json({ ok: false, error: 'Missing or invalid beforeTimestamp cutoff parameter' });
    }
    const safePeerId = String(peerId).replace(/["'<>]/g, '').slice(0, 64);
    const currentOwner = getOwnerId(req);
    if (!currentOwner || currentOwner.startsWith('unauthorized_')) {
        return res.status(401).json({ ok: false, error: 'Unauthorized: account token is required or invalid' });
    }

    // Reject deletion if an uncommitted transaction is pending on disk
    if (fs.existsSync(TRANSACTION_FILE)) {
        const existingTx = loadJson(TRANSACTION_FILE, null);
        if (existingTx && existingTx.status && existingTx.status !== 'committed') {
            return res.status(503).json({
                ok: false,
                error: 'Cannot process deletion: an uncommitted transaction is pending recovery',
                storageHealth
            });
        }
    }

    // Persist permanent deletion cutoff tombstone for this owner and peer
    const cutoffKey = `${currentOwner}:${safePeerId}`;
    const existingCutoff = Number(deletionCutoffs[cutoffKey] || 0);
    const updatedCutoffs = { ...deletionCutoffs };
    let cutoffsChanged = false;
    if (cutoff > existingCutoff) {
        updatedCutoffs[cutoffKey] = cutoff;
        cutoffsChanged = true;
    }

    const snapshotMessages = [...messages];
    const snapshotCutoffs = { ...deletionCutoffs };

    // Transaction journal for atomic deletion
    const txId = 'tx_del_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
    const txRecord = {
        id: txId,
        type: 'delete_peer',
        status: 'pending',
        timestamp: new Date().toISOString(),
        previous: {
            deletionCutoffs: snapshotCutoffs,
            messages: snapshotMessages
        }
    };
    const savedTx = saveJson(TRANSACTION_FILE, txRecord);
    if (!savedTx) {
        return res.status(500).json({ ok: false, error: 'Persistence failure: unable to create deletion transaction journal' });
    }

    if (cutoffsChanged) {
        const cutoffsSaved = saveJson(DELETION_CUTOFFS_FILE, updatedCutoffs);
        if (!cutoffsSaved) {
            try { if (fs.existsSync(TRANSACTION_FILE)) fs.unlinkSync(TRANSACTION_FILE); } catch (_) {}
            return res.status(500).json({ ok: false, error: 'Persistence failure while saving deletion cutoff tombstone' });
        }
        deletionCutoffs = updatedCutoffs;
    }

    const beforeCount = messages.length;
    const filtered = messages.filter(m => {
        const isTarget = String(m.peerId) === safePeerId && (m.owner === currentOwner);
        if (!isTarget) return true;
        return (Number(m.timestamp) || 0) > cutoff;
    });
    const saved = saveJson(MESSAGES_FILE, filtered);
    if (!saved) {
        if (cutoffsChanged) {
            saveJson(DELETION_CUTOFFS_FILE, snapshotCutoffs);
            deletionCutoffs = snapshotCutoffs;
        }
        try { if (fs.existsSync(TRANSACTION_FILE)) fs.unlinkSync(TRANSACTION_FILE); } catch (_) {}
        return res.status(500).json({ ok: false, error: 'Persistence failure while deleting peer messages' });
    }
    messages = filtered;

    let committed = false;
    try {
        if (fs.existsSync(TRANSACTION_FILE)) {
            fs.unlinkSync(TRANSACTION_FILE);
        }
        committed = true;
    } catch (unlinkErr) {
        console.error('[Transaction] Failed to unlink transaction journal after delete:', unlinkErr.message);
    }
    if (!committed) {
        try {
            txRecord.status = 'committed';
            if (saveJson(TRANSACTION_FILE, txRecord)) {
                committed = true;
            }
        } catch (_) {}
    }

    if (!committed) {
        // Rollback memory and disk state to prevent false success on uncommitted journal
        if (cutoffsChanged) {
            saveJson(DELETION_CUTOFFS_FILE, snapshotCutoffs);
            deletionCutoffs = snapshotCutoffs;
        }
        saveJson(MESSAGES_FILE, snapshotMessages);
        messages = snapshotMessages;
        storageHealth = {
            ok: false,
            error: 'Commit journal finalization failure on delete: unable to clear or commit journal on disk',
            lastIncident: new Date().toISOString()
        };
        return res.status(500).json({
            ok: false,
            error: 'Persistence failure: delete transaction could not be safely committed on disk',
            storageHealth
        });
    }
    storageHealth = { ok: true, error: null, lastIncident: null };

    console.log(`[Ghost Cloud] Deleted ${beforeCount - messages.length} messages for peer ${safePeerId} (cutoff: ${cutoff})`);
    res.json({ ok: true, deleted: beforeCount - messages.length, cutoff });
});

// 5. Get Full Conversations (For Mobile Web Viewer)
app.get('/api/conversations', requireAuth, (req, res) => {
    const currentOwner = getOwnerId(req);
    if (!currentOwner || currentOwner.startsWith('unauthorized_')) {
        return res.json({ ok: true, count: 0, conversations: [] });
    }
    const ownerMessages = messages.filter(m => Boolean(m.owner) && m.owner === currentOwner);
    const map = {};
    ownerMessages.forEach(m => {
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

    const convs = Object.values(map).map(c => {
        c.messages.sort((a, b) => {
            const tA = Number(a.timestamp) || 0;
            const tB = Number(b.timestamp) || 0;
            if (tA && tB && Math.abs(tA - tB) > 2000) return tA - tB;
            const idA = parseInt(String(a.id).replace(/\D/g, ''), 10) || 0;
            const idB = parseInt(String(b.id).replace(/\D/g, ''), 10) || 0;
            if (idA && idB && idA !== idB) return idA - idB;
            return tA - tB;
        });
        return c;
    }).sort((a, b) => (b.lastTimestamp || 0) - (a.lastTimestamp || 0));
    res.json({ ok: true, count: convs.length, conversations: convs });
});

function performRevocation(req, res) {
    const clientToken = (req.headers && req.headers['x-ghost-token']) ||
                        (req.body && (req.body.existingToken || req.body.sessionToken || req.body.token)) ||
                        (req.query && (req.query.sessionToken || req.query.token));

    let targetKey = null;
    if (clientToken && typeof clientToken === 'string' && clientToken.trim()) {
        const cleanToken = clientToken.trim();
        for (const [key, acc] of Object.entries(accountSessions)) {
            if (acc.utk === cleanToken || (Array.isArray(acc.utks) && acc.utks.includes(cleanToken)) || (Array.isArray(acc.historicUtks) && acc.historicUtks.includes(cleanToken))) {
                targetKey = key;
                break;
            }
        }
        if (!targetKey) {
            return res.status(401).json({ ok: false, error: 'Unauthorized: invalid account token for revocation' });
        }
    } else {
        // Tokenless revocation: only permitted if at most 1 active account exists AND has NO stored messages or explicit user identity
        const activeEntries = Object.entries(accountSessions).filter(([k, acc]) => acc && !acc.revoked);

        const anyHasDataOrIdentity = activeEntries.some(([k, acc]) => {
            const hasMsgs = messages.some(m => Boolean(m.owner) && m.owner === k);
            const hasExplicitId = Boolean(extractExplicitUserId(acc.cookies));
            return hasMsgs || hasExplicitId;
        }) || (sessionData && Boolean(extractExplicitUserId(sessionData.cookies)));

        if (activeEntries.length > 1 || anyHasDataOrIdentity) {
            return res.status(401).json({ ok: false, error: 'Unauthorized: account token is required to revoke an account with stored data or identity' });
        } else if (activeEntries.length === 1) {
            targetKey = activeEntries[0][0];
        }
    }

    if (targetKey && accountSessions[targetKey]) {
        const acc = accountSessions[targetKey];
        const hasDataOrIdentity = messages.some(m => Boolean(m.owner) && m.owner === targetKey) ||
                                  Boolean(extractExplicitUserId(acc.cookies)) ||
                                  (Array.isArray(acc.utks) && acc.utks.length > 0) ||
                                  (Array.isArray(acc.historicUtks) && acc.historicUtks.length > 0);

        if (hasDataOrIdentity) {
            acc.revoked = true;
            acc.revokedAt = new Date().toISOString();
            if (!Array.isArray(acc.historicUtks)) {
                acc.historicUtks = [];
            }
            if (Array.isArray(acc.utks)) {
                for (const t of acc.utks) {
                    if (t && !acc.historicUtks.includes(t)) {
                        acc.historicUtks.push(t);
                    }
                }
            }
            if (acc.utk && !acc.historicUtks.includes(acc.utk)) {
                acc.historicUtks.push(acc.utk);
            }
            if (!Array.isArray(acc.historicCookies)) {
                acc.historicCookies = [];
            }
            if (acc.cookies && !acc.historicCookies.includes(acc.cookies)) {
                acc.historicCookies.push(acc.cookies);
            }
            if (!Array.isArray(acc.historicPhpSessids)) {
                acc.historicPhpSessids = [];
            }
            const phpMatch = (acc.cookies || '').match(/PHPSESSID=([^;]+)/i);
            if (phpMatch && !acc.historicPhpSessids.includes(phpMatch[1].trim())) {
                acc.historicPhpSessids.push(phpMatch[1].trim());
            }
            acc.utks = [];
            acc.utk = '';
            acc.cookies = '';
            acc.lastUpdated = new Date().toISOString();
        } else {
            delete accountSessions[targetKey];
        }

        if (targetKey) {
            disconnectAccountSocket(targetKey);
        }
        const savedAcc = saveJson(ACCOUNTS_FILE, accountSessions);
        if (!savedAcc) {
            return res.status(500).json({ ok: false, error: 'Persistence failure: unable to update accounts file' });
        }
    }

    // Adjust active server sessionData
    const currentSessionKey = sessionData ? extractStableAccountId(sessionData.cookies, sessionData.utk) : null;
    if (!sessionData) {
        sessionData = { cookies: '', utk: '', userAgent: '', lastUpdated: new Date().toISOString() };
    } else if (currentSessionKey === targetKey || !targetKey || Object.values(accountSessions).filter(a => !a.revoked).length === 0) {
        const remaining = Object.values(accountSessions).filter(a => !a.revoked && (a.utk || a.cookies));
        if (remaining.length > 0) {
            sessionData.cookies = remaining[0].cookies || '';
            sessionData.utk = remaining[0].utk || '';
            sessionData.userAgent = remaining[0].userAgent || '';
            sessionData.lastUpdated = new Date().toISOString();
        } else {
            sessionData.cookies = '';
            sessionData.utk = '';
            sessionData.lastUpdated = new Date().toISOString();
        }
    }

    if (!sessionData.utk && !sessionData.cookies) {
        for (const k of Array.from(accountSockets.keys())) {
            disconnectAccountSocket(k);
        }
        chatSocket = null;
        socketConnected = false;
    }

    const savedSession = saveJson(SESSION_FILE, sessionData);
    if (!savedSession) {
        return res.status(500).json({ ok: false, error: 'Persistence failure: unable to write session file' });
    }

    const remainingActiveCount = Object.values(accountSessions).filter(a => !a.revoked).length;
    addLog(`[API] Session revoked/logged out for account ${targetKey || 'all'}. Remaining active accounts: ${remainingActiveCount}`);
    return res.json({
        ok: true,
        revoked: true,
        accountKey: targetKey,
        remainingAccounts: remainingActiveCount
    });
}

// 5.1 Logout API (Explicit endpoint to revoke session & expire credentials)
app.post('/api/logout', requireAuth, (req, res) => {
    return performRevocation(req, res);
});
app.post('/api/session/logout', requireAuth, (req, res) => {
    return performRevocation(req, res);
});

// 6. Update Session / Cookies (Called automatically by Extension on page visit)
app.post('/api/session', requireAuth, (req, res) => {
    // If storage is degraded from a previous failed transaction or startup recovery, attempt reconciliation before accepting mutations
    if (storageHealth && !storageHealth.ok) {
        if (fs.existsSync(TRANSACTION_FILE)) {
            try {
                const tx = loadJson(TRANSACTION_FILE, null);
                if (tx && tx.status && tx.status !== 'committed' && tx.previous) {
                    let allRestored = true;
                    if (tx.previous.deletionCutoffs && !saveJson(DELETION_CUTOFFS_FILE, tx.previous.deletionCutoffs)) allRestored = false;
                    if (tx.previous.accounts && !saveJson(ACCOUNTS_FILE, tx.previous.accounts)) allRestored = false;
                    if (tx.previous.messages) {
                        const currentDiskMessages = loadJson(MESSAGES_FILE, messages || []);
                        const mergedMessages = reconcileRecoveredMessagesList(tx.previous.messages, currentDiskMessages);
                        if (saveJson(MESSAGES_FILE, mergedMessages)) {
                            messages = mergedMessages;
                        } else {
                            allRestored = false;
                        }
                    }
                    if (tx.previous.session && !saveJson(SESSION_FILE, tx.previous.session)) allRestored = false;
                    if (allRestored) {
                        try { fs.unlinkSync(TRANSACTION_FILE); } catch (_) {}
                        if (tx.previous.deletionCutoffs) deletionCutoffs = tx.previous.deletionCutoffs;
                        accountSessions = tx.previous.accounts || accountSessions;
                        sessionData = tx.previous.session || sessionData;
                        storageHealth = { ok: true, error: null, lastIncident: null };
                    }
                }
            } catch (_) {}
        }
        if (storageHealth && !storageHealth.ok) {
            return res.status(503).json({
                ok: false,
                error: 'Storage degraded: cannot accept session modifications until storage recovery succeeds',
                storageHealth
            });
        }
    }

    // Immediately reject if an uncommitted transaction is pending on disk (even if storageHealth.ok is true)
    // or if storage is currently degraded. Evaluated BEFORE any in-memory state inspection or mutation.
    if (fs.existsSync(TRANSACTION_FILE)) {
        const existingTx = loadJson(TRANSACTION_FILE, null);
        if (existingTx && existingTx.status && existingTx.status !== 'committed') {
            return res.status(503).json({
                ok: false,
                error: 'Cannot update session: an uncommitted transaction is pending recovery',
                storageHealth
            });
        }
    }
    if (storageHealth && !storageHealth.ok) {
        return res.status(503).json({
            ok: false,
            error: 'Cannot update session: storage is currently degraded: ' + (storageHealth.error || 'unspecified'),
            storageHealth
        });
    }

    const { cookies, utk, userAgent } = req.body;
    const prevSession = sessionData ? { ...sessionData } : null;

    const candidateUtk = utk !== undefined ? (typeof utk === 'string' ? utk.trim() : '') : (prevSession ? prevSession.utk : '');
    const candidateUserAgent = userAgent !== undefined ? userAgent : (prevSession ? prevSession.userAgent : '');

    let candidateCookies = '';
    if (cookies !== undefined) {
        candidateCookies = typeof cookies === 'string' ? cookies.trim() : '';
    } else {
        // If cookies not provided: ONLY inherit if candidateUtk belongs to an existing account, or matches prevSession.utk
        let foundAccForUtk = null;
        if (candidateUtk) {
            for (const [k, acc] of Object.entries(accountSessions)) {
                if (acc && !acc.revoked && (acc.utk === candidateUtk || (Array.isArray(acc.utks) && acc.utks.includes(candidateUtk)))) {
                    foundAccForUtk = acc;
                    break;
                }
            }
        }
        if (foundAccForUtk) {
            candidateCookies = foundAccForUtk.cookies || '';
        } else if (prevSession && (!candidateUtk || candidateUtk === prevSession.utk)) {
            candidateCookies = prevSession.cookies || '';
        } else {
            // New/different token without cookies: DO NOT inherit prior account cookies!
            candidateCookies = '';
        }
    }

    // Safe Cookie Merge: If incoming candidateCookies does not include PHPSESSID, but existing account or prevSession has valid PHPSESSID for this user/account, preserve it!
    const incomingPhp = candidateCookies ? candidateCookies.match(/PHPSESSID=([^;]+)/i) : null;
    const incomingUserId = extractExplicitUserId(candidateCookies);
    let fallbackPhp = null;

    if (!incomingPhp) {
        if (candidateUtk) {
            for (const [k, acc] of Object.entries(accountSessions)) {
                if (acc && !acc.revoked && (acc.utk === candidateUtk || (Array.isArray(acc.utks) && acc.utks.includes(candidateUtk)))) {
                    const accUserId = extractExplicitUserId(acc.cookies);
                    if (!incomingUserId || !accUserId || incomingUserId === accUserId) {
                        const m = (acc.cookies || '').match(/PHPSESSID=([^;]+)/i);
                        if (m) { fallbackPhp = m[1]; break; }
                    }
                }
            }
        }
        if (!fallbackPhp && prevSession && prevSession.cookies && (!candidateUtk || candidateUtk === prevSession.utk)) {
            const prevUserId = extractExplicitUserId(prevSession.cookies);
            if (!incomingUserId || !prevUserId || incomingUserId === prevUserId) {
                const m = prevSession.cookies.match(/PHPSESSID=([^;]+)/i);
                if (m) fallbackPhp = m[1];
            }
        }
        if (fallbackPhp) {
            candidateCookies = (candidateCookies ? candidateCookies.replace(/;?\s*$/, '; ') : '') + `PHPSESSID=${fallbackPhp}`;
        }
    }

    // Guard against unauthenticated partial sessions & cross-user pollution:
    const hasIncomingAuth = Boolean(
        (candidateCookies && (candidateCookies.includes('PHPSESSID') || candidateCookies.includes('bc_auth') || candidateCookies.includes('chat-session')))
    );
    const clientRecoveryKey = (req.headers && (req.headers['x-ghost-recovery-key'] || req.headers['x-ghost-admin-key'])) ||
                             (req.body && (req.body.recoveryKey || req.body.adminKey)) ||
                             (req.query && (req.query.recoveryKey || req.query.adminKey));
    const cleanRecoveryKey = clientRecoveryKey ? String(clientRecoveryKey).trim() : null;

    const activeEntries = Object.entries(accountSessions).filter(([k, acc]) => acc && !acc.revoked);
    const matchingAcc = activeEntries.find(([k, acc]) => 
        (candidateUtk && (acc.utk === candidateUtk || (Array.isArray(acc.utks) && acc.utks.includes(candidateUtk)))) ||
        (cleanRecoveryKey && acc.recoveryKey === cleanRecoveryKey)
    );

    const isSingleAccountExplicit = req.body && Boolean(req.body.singleAccount);
    // If partial cookies are sent without complete auth and token is foreign / unauthenticated, reject!
    if (!hasIncomingAuth && !matchingAcc && !isSingleAccountExplicit && activeEntries.length > 0) {
        return res.status(401).json({
            ok: false,
            error: 'Unauthorized: complete session cookies or matching device token required'
        });
    }

    // Cross-user inheritance check:
    if (matchingAcc) {
        const [accKey, accObj] = matchingAcc;
        const accUserId = extractExplicitUserId(accObj.cookies);
        if (incomingUserId && accUserId && incomingUserId !== accUserId) {
            return res.status(403).json({
                ok: false,
                error: 'Forbidden: user identity mismatch for existing account'
            });
        }
    }

    let accKey = extractStableAccountId(candidateCookies, candidateUtk);

    const isRevocation = (cookies !== undefined || utk !== undefined) && !candidateCookies && !candidateUtk;
    if (isRevocation) {
        return performRevocation(req, res);
    }

    // Cross-Account Token Mismatch Protection (F3):
    // A token actively bound to one account cannot be claimed or hijacked by a different user account
    if (candidateUtk && accKey) {
        for (const [k, acc] of Object.entries(accountSessions)) {
            if (k !== accKey && acc && !acc.revoked) {
                const ownsActive = acc.utk === candidateUtk || (Array.isArray(acc.utks) && acc.utks.includes(candidateUtk));
                if (ownsActive) {
                    const ownerUserId = extractExplicitUserId(acc.cookies);
                    const currentUserId = extractExplicitUserId(candidateCookies);
                    if (ownerUserId && currentUserId && ownerUserId !== currentUserId) {
                        return res.status(409).json({
                            ok: false,
                            error: `Conflict: token "${candidateUtk}" is actively bound to account ${k}; cross-account token transfer between different users is prohibited`
                        });
                    }
                }
            }
        }
    }

    // 1. Authorization & Anti-Hijacking Check: BEFORE TOUCHING DISK OR MEMORY
    // If accKey is already registered with existing tokens (active or historic/tombstoned),
    // enrolling a NEW token requires proving ownership of the account.
    // If the account was revoked, revoked tokens and expired session data are expired for all purposes and cannot authorize recovery.
    // An independent recovery credential (account recoveryKey or admin key) is required.
    if (accKey && accountSessions[accKey]) {
        const acc = accountSessions[accKey];
        const historicTokens = Array.isArray(acc.historicUtks) ? acc.historicUtks : [];
        const historicCookies = Array.isArray(acc.historicCookies) ? acc.historicCookies : [];
        const historicPhpSessids = Array.isArray(acc.historicPhpSessids) ? acc.historicPhpSessids : [];
        const activeTokens = Array.isArray(acc.utks) ? acc.utks : [];
        if (acc.utk && !activeTokens.includes(acc.utk) && !acc.revoked) {
            activeTokens.push(acc.utk);
        }

        const reqToken = (req.headers && req.headers['x-ghost-token']) ||
                         (req.body && (req.body.existingToken || req.body.sessionToken));
        const cleanReqToken = reqToken ? String(reqToken).trim() : null;

        const clientRecoveryKey = (req.headers && (req.headers['x-ghost-recovery-key'] || req.headers['x-ghost-admin-key'])) ||
                                 (req.body && (req.body.recoveryKey || req.body.adminKey)) ||
                                 (req.query && (req.query.recoveryKey || req.query.adminKey));
        const cleanRecoveryKey = clientRecoveryKey ? String(clientRecoveryKey).trim() : null;

        const adminKey = getAdminRecoveryKey();
        const isRecoveryAuthorized = Boolean(
            (cleanRecoveryKey && (
                (acc.recoveryKey && cleanRecoveryKey === acc.recoveryKey) ||
                (adminKey && cleanRecoveryKey === adminKey)
            )) ||
            checkAuth(req)
        );

        if (acc.revoked) {
            // Case 1: Re-enrolling a REVOKED account
            // 1. Revoked session tokens are expired unless request has independent recovery/secret authorization.
            if (cleanReqToken && historicTokens.includes(cleanReqToken) && !isRecoveryAuthorized) {
                return res.status(403).json({
                    ok: false,
                    error: 'Forbidden: revoked session token cannot authorize account recovery or replacement. Independent recovery credential required.'
                });
            }

            // 2. A revoked historical token can only be reactivated if recovery is authorized.
            if (candidateUtk && historicTokens.includes(candidateUtk) && !isRecoveryAuthorized) {
                return res.status(403).json({
                    ok: false,
                    error: 'Forbidden: revoked token is expired for all purposes and cannot reactivate itself as an account token'
                });
            }

            // 3. Expired session cookies cannot authorize recovery or grant authority.
            const candidatePhp = (candidateCookies || '').match(/PHPSESSID=([^;]+)/i);
            if (candidatePhp && historicPhpSessids.includes(candidatePhp[1].trim()) && !isRecoveryAuthorized) {
                return res.status(403).json({
                    ok: false,
                    error: 'Forbidden: expired session cookie from revoked session cannot authorize account recovery'
                });
            }

            // 4. Re-enrolling or recovering a revoked account strictly requires independent recovery authorization.
            if (!candidateUtk || !isRecoveryAuthorized) {
                return res.status(403).json({
                    ok: false,
                    error: 'Forbidden: re-enrolling a revoked account requires independent recovery authorization (recoveryKey or adminKey)'
                });
            }

            // If recovery is authorized, un-revoke the account!
            acc.revoked = false;
            acc.revokedAt = null;
        } else {
            // Case 2: ACTIVE account
            // 1. If candidateUtk is already one of the active tokens, it's an authenticated refresh/update.
            const isExistingActiveToken = candidateUtk && activeTokens.includes(candidateUtk);

            // 2. If enrolling a new token for an active account:
            //    - The candidate token cannot be an old revoked/historic token.
            if (candidateUtk && historicTokens.includes(candidateUtk) && !isExistingActiveToken) {
                return res.status(403).json({ ok: false, error: 'Forbidden: historic token cannot be reused as active token' });
            }

            //    - A historic/revoked token cannot authorize new token enrollment.
            if (cleanReqToken && historicTokens.includes(cleanReqToken) && !activeTokens.includes(cleanReqToken)) {
                return res.status(403).json({
                    ok: false,
                    error: 'Forbidden: revoked session token cannot authorize new token enrollment. Active account token or independent recovery credential required.'
                });
            }

            //    - The client must prove ownership using one of the current active tokens OR recovery authorization.
            const hasProvenActiveToken = cleanReqToken && activeTokens.includes(cleanReqToken);

            if (activeTokens.length > 0 && !isExistingActiveToken && !hasProvenActiveToken && !isRecoveryAuthorized) {
                return res.status(403).json({ ok: false, error: 'Forbidden: existing account token required to enroll new token for this account' });
            }
        }
    }

    let changed = false;
    if (!sessionData) {
        changed = true;
    } else {
        if (candidateCookies !== sessionData.cookies) {
            changed = true;
        }
        if (candidateUtk !== sessionData.utk) {
            changed = true;
        }
        if (candidateUserAgent !== sessionData.userAgent) {
            changed = true;
        }
    }

    if (changed) {
        // Deep clone snapshots of in-memory states before making mutations
        const snapshotSessionData = prevSession ? JSON.parse(JSON.stringify(prevSession)) : null;
        const snapshotAccountSessions = JSON.parse(JSON.stringify(accountSessions));
        const snapshotMessages = JSON.parse(JSON.stringify(messages));

        sessionData = {
            cookies: candidateCookies,
            utk: candidateUtk,
            userAgent: candidateUserAgent,
            lastUpdated: new Date().toISOString()
        };

        // 1. Prepare in-memory updates for accountSessions & messages
        let migrated = false;
        if (accKey) {
            const prevAccKey = prevSession ? extractStableAccountId(prevSession.cookies, prevSession.utk) : null;
            const prevUserId = prevSession ? extractExplicitUserId(prevSession.cookies) : null;
            const currentUserId = extractExplicitUserId(sessionData.cookies);
            const isDifferentUser = Boolean(prevUserId && currentUserId && prevUserId !== currentUserId);

            const isSameAccountRenewal = Boolean(prevSession && prevSession.utk && sessionData.utk &&
                                         prevSession.utk === sessionData.utk && prevAccKey && prevAccKey !== accKey && !isDifferentUser);

            const clientRecoveryKey = (req.headers && (req.headers['x-ghost-recovery-key'] || req.headers['x-ghost-admin-key'])) ||
                                     (req.body && (req.body.recoveryKey || req.body.adminKey)) ||
                                     (req.query && (req.query.recoveryKey || req.query.adminKey));
            const cleanRecoveryKey = clientRecoveryKey ? String(clientRecoveryKey).trim() : null;

            let renewalSourceKey = null;
            if (isSameAccountRenewal) {
                if (prevAccKey && accountSessions[prevAccKey]) {
                    renewalSourceKey = prevAccKey;
                } else {
                    for (const [k, acc] of Object.entries(accountSessions)) {
                        if (!acc.revoked && (
                            (sessionData.utk && (acc.utk === sessionData.utk || (Array.isArray(acc.utks) && acc.utks.includes(sessionData.utk)))) ||
                            (Array.isArray(acc.legacyKeys) && acc.legacyKeys.includes(prevAccKey)) ||
                            (prevSession && prevSession.cookies && acc.cookies === prevSession.cookies) ||
                            (prevSession && prevSession.utk && acc.utk === prevSession.utk)
                        )) {
                            renewalSourceKey = k;
                            break;
                        }
                    }
                }
            }

            if (isSameAccountRenewal && renewalSourceKey && accountSessions[renewalSourceKey]) {
                accountSessions[accKey] = accountSessions[renewalSourceKey];
                accountSessions[accKey].accountKey = accKey;
                accountSessions[accKey].cookies = sessionData.cookies;
                accountSessions[accKey].utk = sessionData.utk;
                accountSessions[accKey].userAgent = sessionData.userAgent;
                accountSessions[accKey].lastUpdated = sessionData.lastUpdated;
                accountSessions[accKey].revoked = false;
                if (!accountSessions[accKey].recoveryKey) {
                    accountSessions[accKey].recoveryKey = generateRecoveryKey();
                }
                if (!Array.isArray(accountSessions[accKey].utks)) accountSessions[accKey].utks = [];
                if (!Array.isArray(accountSessions[accKey].historicUtks)) accountSessions[accKey].historicUtks = [];
                if (!Array.isArray(accountSessions[accKey].legacyKeys)) accountSessions[accKey].legacyKeys = [];
                if (renewalSourceKey !== accKey && !accountSessions[accKey].legacyKeys.includes(renewalSourceKey)) {
                    accountSessions[accKey].legacyKeys.push(renewalSourceKey);
                }
                if (prevAccKey && prevAccKey !== accKey && !accountSessions[accKey].legacyKeys.includes(prevAccKey)) {
                    accountSessions[accKey].legacyKeys.push(prevAccKey);
                }
                if (sessionData.utk) {
                    if (!accountSessions[accKey].utks.includes(sessionData.utk)) {
                        accountSessions[accKey].utks.push(sessionData.utk);
                    }
                    if (!accountSessions[accKey].historicUtks.includes(sessionData.utk)) {
                        accountSessions[accKey].historicUtks.push(sessionData.utk);
                    }
                }
                if (renewalSourceKey !== accKey) {
                    delete accountSessions[renewalSourceKey];
                }
            } else if (!accountSessions[accKey]) {
                accountSessions[accKey] = {
                    accountKey: accKey,
                    cookies: sessionData.cookies,
                    utk: sessionData.utk,
                    userAgent: sessionData.userAgent,
                    utks: [sessionData.utk].filter(Boolean),
                    historicUtks: [sessionData.utk].filter(Boolean),
                    recoveryKey: generateRecoveryKey(),
                    revoked: false,
                    lastUpdated: sessionData.lastUpdated
                };
            } else {
                accountSessions[accKey].revoked = false;
                accountSessions[accKey].cookies = sessionData.cookies;
                accountSessions[accKey].utk = sessionData.utk;
                accountSessions[accKey].userAgent = sessionData.userAgent;
                accountSessions[accKey].lastUpdated = sessionData.lastUpdated;
                if (!accountSessions[accKey].recoveryKey) {
                    accountSessions[accKey].recoveryKey = generateRecoveryKey();
                } else if (req.body.newRecoveryKey) {
                    accountSessions[accKey].recoveryKey = String(req.body.newRecoveryKey).trim();
                }
                if (!Array.isArray(accountSessions[accKey].utks)) accountSessions[accKey].utks = [];
                if (!Array.isArray(accountSessions[accKey].historicUtks)) accountSessions[accKey].historicUtks = [];
                if (sessionData.utk) {
                    if (!accountSessions[accKey].utks.includes(sessionData.utk)) {
                        accountSessions[accKey].utks.push(sessionData.utk);
                    }
                    if (!accountSessions[accKey].historicUtks.includes(sessionData.utk)) {
                        accountSessions[accKey].historicUtks.push(sessionData.utk);
                    }
                }
            }

            // Disassociate this token from any other account to maintain unique token mapping
            if (sessionData.utk) {
                for (const [key, acc] of Object.entries(accountSessions)) {
                    if (key !== accKey && Array.isArray(acc.utks)) {
                        acc.utks = acc.utks.filter(t => t !== sessionData.utk);
                        if (acc.utk === sessionData.utk) {
                            acc.utk = acc.utks[0] || '';
                        }
                    }
                }
            }

            // Safe Migration:
            // 1. If same account renewal (same UTK with rotated PHPSESSID), migrate from prevAccKey, renewalSourceKey, or legacy keys to accKey.
            // 2. If message is owned by current account's registered UTK(s), migrate to accKey.
            // NEVER migrate messages if the user ID changed (C to D)!
            const isAccountUtk = mOwner => (mOwner === sessionData.utk || (Array.isArray(accountSessions[accKey].utks) && accountSessions[accKey].utks.includes(mOwner))) &&
                                          mOwner !== accKey && !isDifferentUser;
            const isRenewalOwner = mOwner => Boolean(
                isSameAccountRenewal && mOwner && (
                    mOwner === prevAccKey ||
                    mOwner === renewalSourceKey ||
                    (accountSessions[accKey].legacyKeys && accountSessions[accKey].legacyKeys.includes(mOwner)) ||
                    (prevSession && prevSession.cookies && mOwner === prevSession.cookies.match(/PHPSESSID=([^;]+)/i)?.[1]?.trim()) ||
                    (prevSession && prevSession.utk && mOwner === prevSession.utk)
                )
            );
            messages.forEach(m => {
                if (m.owner) {
                    if (isRenewalOwner(m.owner)) {
                        m.owner = accKey;
                        migrated = true;
                    } else if (isAccountUtk(m.owner)) {
                        m.owner = accKey;
                        migrated = true;
                    }
                }
            });

            // Single-Account Enforcement:
            if (req.body.singleAccount || req.body.replaceOthers) {
                for (const [k, acc] of Object.entries(accountSessions)) {
                    if (k !== accKey && acc) {
                        acc.revoked = true;
                        disconnectAccountSocket(k);
                    }
                }
                addLog(`[Accounts] Single-Account Mode: Set ${accKey} as the only active cloud account.`);
            }

            // Write transaction journal to disk to guarantee recoverability across restarts BEFORE touching ANY state files!
            const txRecord = {
                id: 'tx_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8),
                status: 'pending',
                timestamp: Date.now(),
                accKey: accKey,
                previous: {
                    accounts: snapshotAccountSessions,
                    messages: snapshotMessages,
                    session: snapshotSessionData
                }
            };
            const savedTx = saveJson(TRANSACTION_FILE, txRecord);
            if (!savedTx) {
                // Journal write failed! Abort before touching ANY state files on disk!
                sessionData = snapshotSessionData;
                accountSessions = snapshotAccountSessions;
                messages = snapshotMessages;
                storageHealth = {
                    ok: false,
                    error: 'Transaction aborted: unable to write transaction journal to disk',
                    lastIncident: new Date().toISOString()
                };
                return res.status(500).json({
                    ok: false,
                    error: 'Persistence failure: unable to write transaction journal',
                    storageHealth
                });
            }

            // 2. Persist ACCOUNTS_FILE FIRST!
            // If writing accounts fails, session.json and messages.json on disk were never touched.
            const savedAcc = saveJson(ACCOUNTS_FILE, accountSessions);
            if (!savedAcc) {
                try { if (fs.existsSync(TRANSACTION_FILE)) fs.unlinkSync(TRANSACTION_FILE); } catch (_) {}
                sessionData = snapshotSessionData;
                accountSessions = snapshotAccountSessions;
                messages = snapshotMessages;
                return res.status(500).json({ ok: false, error: 'Persistence failure: unable to write accounts file' });
            }

            // 3. Persist MESSAGES_FILE SECOND if migrated
            if (migrated) {
                const savedMessages = saveJson(MESSAGES_FILE, messages);
                if (!savedMessages) {
                    // Rollback accounts file on disk
                    const rolledBack = saveJson(ACCOUNTS_FILE, snapshotAccountSessions);
                    sessionData = snapshotSessionData;
                    accountSessions = snapshotAccountSessions;
                    messages = snapshotMessages;
                    if (!rolledBack) {
                        storageHealth = {
                            ok: false,
                            error: 'Multi-file rollback failure: accounts.json could not be restored and is desynchronized until recovery/restart',
                            lastIncident: new Date().toISOString()
                        };
                        txRecord.status = 'rollback_failed';
                        const savedTxStatus = saveJson(TRANSACTION_FILE, txRecord);
                        if (!savedTxStatus) {
                            console.error('[Transaction] Failed to update transaction status to rollback_failed');
                        }
                        return res.status(500).json({ ok: false, error: 'Persistence failure: unable to write messages file and accounts rollback failed', storageHealth });
                    }
                    try { if (fs.existsSync(TRANSACTION_FILE)) fs.unlinkSync(TRANSACTION_FILE); } catch (_) {}
                    return res.status(500).json({ ok: false, error: 'Persistence failure: unable to write messages file' });
                }
            }
        }

        // 4. Persist SESSION_FILE LAST!
        const saved = saveJson(SESSION_FILE, sessionData);
        if (!saved) {
            let rollbackFailed = false;
            if (accKey) {
                // Rollback accounts file on disk
                if (!saveJson(ACCOUNTS_FILE, snapshotAccountSessions)) rollbackFailed = true;
                if (migrated) {
                    if (!saveJson(MESSAGES_FILE, snapshotMessages)) rollbackFailed = true;
                }
            }
            sessionData = snapshotSessionData;
            accountSessions = snapshotAccountSessions;
            messages = snapshotMessages;
            if (rollbackFailed) {
                storageHealth = {
                    ok: false,
                    error: 'Multi-file rollback failure: files could not be restored and are desynchronized until recovery/restart',
                    lastIncident: new Date().toISOString()
                };
                if (typeof txRecord !== 'undefined') {
                    txRecord.status = 'rollback_failed';
                    const savedTxStatus = saveJson(TRANSACTION_FILE, txRecord);
                    if (!savedTxStatus) {
                        console.error('[Transaction] Failed to update transaction status to rollback_failed');
                    }
                }
                return res.status(500).json({ ok: false, error: 'Persistence failure: unable to write session file and rollback failed', storageHealth });
            }
            try { if (fs.existsSync(TRANSACTION_FILE)) fs.unlinkSync(TRANSACTION_FILE); } catch (_) {}
            return res.status(500).json({ ok: false, error: 'Persistence failure: unable to write session file' });
        }

        // Transaction successfully committed! Clear journal and reset storage health
        let committed = false;
        try {
            if (fs.existsSync(TRANSACTION_FILE)) {
                fs.unlinkSync(TRANSACTION_FILE);
            }
            committed = true;
        } catch (unlinkErr) {
            console.error('[Transaction] Failed to unlink transaction journal after commit:', unlinkErr.message);
        }
        if (!committed) {
            try {
                if (typeof txRecord !== 'undefined') {
                    txRecord.status = 'committed';
                    if (saveJson(TRANSACTION_FILE, txRecord)) {
                        committed = true;
                    }
                }
            } catch (_) {}
        }

        if (!committed) {
            // Failed both to unlink journal and to mark committed!
            // Revert state on disk and in memory to prevent false success.
            if (accKey) {
                saveJson(ACCOUNTS_FILE, snapshotAccountSessions);
                if (migrated) saveJson(MESSAGES_FILE, snapshotMessages);
            }
            saveJson(SESSION_FILE, snapshotSessionData);
            sessionData = snapshotSessionData;
            accountSessions = snapshotAccountSessions;
            messages = snapshotMessages;
            storageHealth = {
                ok: false,
                error: 'Commit journal finalization failure: unable to clear or commit journal on disk',
                lastIncident: new Date().toISOString()
            };
            return res.status(500).json({
                ok: false,
                error: 'Persistence failure: transaction could not be safely committed on disk',
                storageHealth
            });
        }
        storageHealth = { ok: true, error: null, lastIncident: null };

        const hasPhp = (sessionData.cookies || '').includes('PHPSESSID');
        addLog(`[API] Session updated (Cookies: ${(sessionData.cookies || '').length} chars, PHPSESSID: ${hasPhp ? 'YES' : 'NO'}). Reconnecting socket...`);
        console.log(`[API] Session updated (Cookies: ${(sessionData.cookies || '').length} chars, PHPSESSID: ${hasPhp}). Reconnecting socket...`);

        if (sessionData.cookies && sessionData.utk) {
            initChatSocket();
            setTimeout(pollArabicChatOnce, 300);
        } else if (chatSocket) {
            try { chatSocket.disconnect(); } catch (e) {}
            chatSocket = null;
            socketConnected = false;
        }
    }

    const hasPhp = (sessionData.cookies || '').includes('PHPSESSID');
    const cookieKeys = sessionData.cookies ? sessionData.cookies.split(';').map(c => c.trim().split('=')[0]).filter(Boolean) : [];
    const hasAuthCookies = Boolean(
        sessionData.cookies && (
            hasPhp ||
            sessionData.cookies.includes('bc_auth') ||
            sessionData.cookies.includes('chat-session') ||
            cookieKeys.length > 0
        )
    );
    res.json({
        ok: true,
        changed: changed,
        message: changed ? 'Session updated successfully' : 'Session unchanged',
        socketConnected: socketConnected,
        hasPhpsessid: hasPhp || hasAuthCookies,
        hasSessionCookies: hasAuthCookies,
        recoveryKey: (accKey && accountSessions[accKey]) ? accountSessions[accKey].recoveryKey : undefined
    });
});

// Clean dead/unauthorized/non-current accounts
app.post('/api/accounts/clean', requireAuth, (req, res) => {
    let purgedCount = 0;
    const currentOwner = getOwnerId(req);
    const keepKey = req.body.keepKey || (currentOwner && !currentOwner.startsWith('unauthorized_') ? currentOwner : null);

    for (const [k, acc] of Object.entries(accountSessions)) {
        const isTarget = keepKey ? (k !== keepKey) : (acc.revoked || (accountSockets.get(k) && accountSockets.get(k).authExpired));
        if (isTarget) {
            disconnectAccountSocket(k);
            delete accountSessions[k];
            purgedCount++;
        }
    }

    if (keepKey && accountSessions[keepKey]) {
        sessionData.cookies = accountSessions[keepKey].cookies || '';
        sessionData.utk = accountSessions[keepKey].utk || '';
        sessionData.userAgent = accountSessions[keepKey].userAgent || '';
        sessionData.lastUpdated = accountSessions[keepKey].lastUpdated || new Date().toISOString();
        saveJson(SESSION_FILE, sessionData);
    }

    saveJson(ACCOUNTS_FILE, accountSessions);
    addLog(`[Accounts] Cleaned up ${purgedCount} dead or inactive account(s) from cloud relay.`);
    res.json({ ok: true, purgedCount, activeAccounts: Object.keys(accountSessions) });
});

// 7. Test Message Injector (For debugging and manual verification)
app.post('/api/test-msg', requireAuth, (req, res) => {
    const { peer, name, message } = req.body;
    const testPeer = String(peer || '9999');
    const testName = name || 'تجربة سحابية 👻';
    const testText = message || 'هذه رسالة واردة تم التقاطها عبر السيرفر السحابي أثناء إغلاق المتصفح!';

    const testOwner = getOwnerId(req);
    const testMsg = {
        id: 'msg_test_' + Date.now(),
        seq: nextOwnerSeq(testOwner),
        peerId: testPeer,
        name: testName,
        avatar: 'default_images/avatar/default_avatar.png',
        text: testText,
        html: `<div class="target_private">${testText}</div>`,
        time: new Date().toLocaleTimeString('ar-EG', { hour: '2-digit', minute: '2-digit' }),
        timestamp: Date.now(),
        type: 'received',
        synced: false,
        owner: testOwner
    };

    const candidate = [...messages, testMsg];
    const saved = saveJson(MESSAGES_FILE, candidate);
    if (!saved) {
        return res.status(500).json({ ok: false, error: 'Persistence failure: unable to write messages file' });
    }
    messages = candidate;

    res.json({
        ok: true,
        message: 'Test message added to cloud storage',
        data: testMsg
    });
});

// 8. Clear Messages API
app.post('/api/clear', requireAuth, (req, res) => {
    const saved = saveJson(MESSAGES_FILE, []);
    if (!saved) {
        return res.status(500).json({ ok: false, error: 'Persistence failure: unable to write messages file' });
    }
    messages = [];
    res.json({ ok: true, message: 'All messages cleared' });
});

// Start Server
app.listen(PORT, '0.0.0.0', () => {
    console.log(`=================================================`);
    console.log(`👻 Ghost Cloud Relay Server is RUNNING on port ${PORT}`);
    console.log(`🔒 Secret Key: configured (${GHOST_SECRET.length} chars)`);
    console.log(`🌐 Target: ${SITE_URL}${SOCKET_PATH}`);
    console.log(`=================================================`);

    // Initial socket connect & Polling Engine boot
    initChatSocket();
    startPollingEngine();

    // Render Free Tier Keep-Alive: Ping self every 4 minutes to prevent sleeping
    const SELF_URL = process.env.RENDER_EXTERNAL_URL || 'https://ghost-cloud-relay.onrender.com';
    setInterval(() => {
        fetch(`${SELF_URL}/api/status`).catch(() => {});
    }, 4 * 60 * 1000);
});
