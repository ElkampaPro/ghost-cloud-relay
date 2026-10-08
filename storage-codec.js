'use strict';
const { createCipheriv, createDecipheriv, createHash, randomBytes } = require('crypto');

// Deployment-owned key, distinct from login/session secrets. Never generate or
// rotate it implicitly: losing it must not silently replace unreadable data.
function createStorageCodec(encodedKey) {
    if (!encodedKey) return { enabled: false, encode: (_identity, value) => JSON.stringify(value), decode: (_identity, text) => {
        const value = JSON.parse(text);
        if (value && value.format === 'ghost-storage-aes-gcm-v1') throw Error('Storage encryption key is required');
        return value;
    } };
    const rawSecret = String(encodedKey);
    let key;
    if (/^[A-Za-z0-9+/]{43}=$/.test(rawSecret)) {
        key = Buffer.from(rawSecret, 'base64');
    } else {
        if (rawSecret.length < 32) throw Error('GHOST_STORAGE_KEY must contain at least 32 characters');
        // Render-generated secrets are high-entropy strings but are not
        // guaranteed to use one specific encoding. Derive a fixed AES key.
        key = createHash('sha256').update(rawSecret, 'utf8').digest();
    }
    return {
        enabled: true,
        encode(identity, value) {
            const iv = randomBytes(12), cipher = createCipheriv('aes-256-gcm', key, iv);
            cipher.setAAD(Buffer.from(identity, 'utf8'));
            const bytes = Buffer.concat([cipher.update(JSON.stringify(value), 'utf8'), cipher.final()]);
            return JSON.stringify({ format: 'ghost-storage-aes-gcm-v1', iv: iv.toString('base64'), tag: cipher.getAuthTag().toString('base64'), bytes: bytes.toString('base64') });
        },
        decode(identity, text) {
            const value = JSON.parse(text);
            if (!value || value.format !== 'ghost-storage-aes-gcm-v1') return value; // Migration: legacy JSON remains readable.
            const iv = Buffer.from(value.iv || '', 'base64'), tag = Buffer.from(value.tag || '', 'base64');
            if (iv.length !== 12 || tag.length !== 16 || typeof value.bytes !== 'string') throw Error('Invalid encrypted storage envelope');
            const cipher = createDecipheriv('aes-256-gcm', key, iv);
            cipher.setAAD(Buffer.from(identity, 'utf8'));
            cipher.setAuthTag(tag);
            return JSON.parse(Buffer.concat([cipher.update(Buffer.from(value.bytes, 'base64')), cipher.final()]).toString('utf8'));
        }
    };
}
module.exports = { createStorageCodec };
