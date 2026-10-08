'use strict';
const assert = require('node:assert/strict');
const { createStorageCodec } = require('../storage-codec');

const key = Buffer.alloc(32, 7).toString('base64');
const codec = createStorageCodec(key);
const input = { accounts: [{ id: '100', token: 'fixture' }], count: 1 };
const encoded = codec.encode('accounts.json', input);

assert.notEqual(encoded, JSON.stringify(input), 'encrypted storage must not contain plaintext JSON');
assert.deepEqual(codec.decode('accounts.json', encoded), input, 'encrypted storage must round-trip');
assert.throws(() => codec.decode('messages.json', encoded), /auth|authenticate|Unsupported state/i,
    'file identity must be authenticated as AAD');
assert.deepEqual(createStorageCodec('').decode('legacy.json', JSON.stringify(input)), input,
    'legacy plaintext JSON must remain readable during migration');
assert.throws(() => createStorageCodec('').decode('accounts.json', encoded), /key is required/i,
    'encrypted data must never be opened without its deployment key');
const generatedSecretCodec = createStorageCodec('render-generated-secret-value-1234567890');
assert.deepEqual(generatedSecretCodec.decode('messages.json', generatedSecretCodec.encode('messages.json', input)), input,
    'high-entropy platform-generated secrets must be supported');
assert.throws(() => createStorageCodec('too-short'), /at least 32 characters/i,
    'weak storage secrets must fail closed');

console.log('Storage encryption and legacy migration checks passed');
