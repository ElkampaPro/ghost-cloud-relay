'use strict';

function normalizeOrigin(value) {
    if (typeof value !== 'string' || !value.trim()) return null;
    try {
        const url = new URL(value.trim());
        if (url.protocol === 'chrome-extension:') {
            return /^[a-p]{32}$/i.test(url.hostname)
                ? `chrome-extension://${url.hostname.toLowerCase()}`
                : null;
        }
        if (url.protocol !== 'https:' && url.protocol !== 'http:') {
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
        const normalized = normalizeOrigin(origin);
        return Boolean(normalized && normalizedAllowed.has(normalized));
    };
}

module.exports = { createCorsOriginChecker, normalizeOrigin };
