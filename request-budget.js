'use strict';

function createRequestBudget({ now = Date.now, capacity = 2048, authenticatedLimit = 600, anonymousLimit = 60 } = {}) {
    const buckets = new Map();
    return function requestBudget(req, res, next) {
        // Do not trust an arbitrary client-supplied X-Forwarded-For value.
        const key = String(req.ip || req.socket && req.socket.remoteAddress || 'unknown') + ':' + (req.ghostAuthenticated ? 'authenticated' : 'anonymous');
        const time = now();
        let bucket = buckets.get(key);
        if (!bucket || time >= bucket.resetAt) {
            if (buckets.size >= capacity) {
                for (const [id, old] of buckets) if (time >= old.resetAt) buckets.delete(id);
                if (buckets.size >= capacity && !buckets.has(key)) {
                    res.set('Retry-After', '60');
                    return res.status(429).json({ ok: false, error: 'Request budget exhausted' });
                }
            }
            bucket = { resetAt: time + 60000, count: 0 };
            buckets.set(key, bucket);
        }
        bucket.count++;
        const limit = req.ghostAuthenticated ? authenticatedLimit : anonymousLimit;
        if (bucket.count > limit) {
            res.set('Retry-After', String(Math.max(1, Math.ceil((bucket.resetAt - time) / 1000))));
            return res.status(429).json({ ok: false, error: 'Too many requests; retry later' });
        }
        next();
    };
}
module.exports = { createRequestBudget };
