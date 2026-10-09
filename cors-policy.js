'use strict';

const EXTENSION_ORIGIN_RE = /^chrome-extension:\/\/[a-p]{32}$/i;

function normalizeOrigin(value) {
    if (typeof value !== 'string' || !value.trim()) return null;
    try {
        const url = new URL(value.trim());
        if (url.protocol !== 'https:' && url.protocol !== 'http:' && url.protocol !== 'chrome-extension:') {
            return null;
        }
        return url.origin.toLowerCase();
    } catch (_) {
        return null;
    }
}

function createCorsOriginChecker(allowedOrigins, options = {}) {
    const normalizedAllowed = new Set(
        Array.from(allowedOrigins || [])
            .map(normalizeOrigin)
            .filter(Boolean)
    );
    const allowNullOrigin = options.allowNullOrigin === true;

    return function isAllowedCorsOrigin(origin) {
        // Native clients and server-to-server calls normally omit Origin.
        if (!origin) return true;
        if (origin === 'null') return allowNullOrigin;
        if (EXTENSION_ORIGIN_RE.test(origin)) return true;
        const normalized = normalizeOrigin(origin);
        return Boolean(normalized && normalizedAllowed.has(normalized));
    };
}

module.exports = { createCorsOriginChecker, normalizeOrigin };
