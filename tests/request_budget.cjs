'use strict';
const assert = require('node:assert/strict');
const { createRequestBudget } = require('../request-budget');

let now = 0;
const budget = createRequestBudget({ now: () => now, capacity: 8, authenticatedLimit: 2, anonymousLimit: 1 });

function request(authenticated, ip = '127.0.0.1') {
    let status = 200;
    let passed = false;
    const headers = {};
    budget(
        { ip, ghostAuthenticated: authenticated, socket: {} },
        {
            set(name, value) { headers[name] = value; },
            status(value) { status = value; return this; },
            json() { return this; }
        },
        () => { passed = true; }
    );
    return { status, passed, headers };
}

assert.equal(request(false).passed, true);
assert.equal(request(false).status, 429, 'anonymous callers must use the lower budget');
assert.equal(request(true).passed, true);
assert.equal(request(true).passed, true);
assert.equal(request(true).status, 429, 'authenticated callers must still be bounded');
assert.equal(request(true, '127.0.0.2').passed, true, 'independent clients must not share a bucket');
now = 60001;
assert.equal(request(false).passed, true, 'budget must reset after its time window');

console.log('Request budget isolation and reset checks passed');
