'use strict';

const assert = require('node:assert/strict');
const { createCorsOriginChecker, normalizeOrigin } = require('../cors-policy');

const isAllowed = createCorsOriginChecker(new Set([
    'https://arabic.chat',
    'https://www.arabic.chat'
]));

assert.equal(isAllowed(undefined), true, 'Native requests without Origin must remain supported');
assert.equal(isAllowed('https://arabic.chat'), true);
assert.equal(isAllowed('https://www.arabic.chat'), true);
assert.equal(isAllowed('chrome-extension://abcdefghijklmnopabcdefghijklmnop'), true);

assert.equal(isAllowed('https://evil.example'), false);
assert.equal(isAllowed('https://arabic.chat.evil.example'), false);
assert.equal(isAllowed('https://evil-arabic.chat'), false);
assert.equal(isAllowed('null'), false, 'Opaque origins require an explicit opt-in');
assert.equal(isAllowed('javascript:alert(1)'), false);

assert.equal(normalizeOrigin('https://www.arabic.chat/path?q=1'), 'https://www.arabic.chat');
assert.equal(normalizeOrigin('not an origin'), null);

console.log('CORS origin allowlist checks passed');
