'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');
const { createStorageCodec } = require('../storage-codec');
const { createAtomicJsonStore } = require('../atomic-json-store');
const files = new Map(); let failRename = false, clock = 0;
const fs = { existsSync: file => file === '/data' || files.has(file), mkdirSync() {},
    writeFileSync(file, data) { files.set(file, data); },
    readFileSync: file => files.get(file),
    renameSync(from, to) { if (failRename) throw Error('disk unavailable'); files.set(to, files.get(from)); files.delete(from); },
    copyFileSync(from, to) { files.set(to, files.get(from)); }, unlinkSync: file => files.delete(file) };
const codec = createStorageCodec('test-only-storage-key-with-at-least-32-characters');
const store = createAtomicJsonStore({ fs, path: path.posix, codec, logger: { error() {} }, now: () => clock++ });
const target = '/data/messages.json';
assert.equal(store.save(target, [{ id: 'old' }]), true);
assert.ok(!files.get(target).includes('"old"'));
assert.deepEqual(store.load(target), [{ id: 'old' }]);
const original = files.get(target);
failRename = true;
assert.equal(store.save(target, [{ id: 'new' }]), false);
assert.equal(files.get(target), original, 'Failed commit must preserve the previous encrypted snapshot');
assert.equal([...files.keys()].some(file => file.includes('.tmp_')), false);
files.set(target, 'corrupt');
assert.throws(() => store.load(target, []), /startup aborted/);
assert.equal(files.get(target), 'corrupt', 'Authentication failure must never overwrite the source');
assert.ok([...files.keys()].some(file => file.includes('.corrupt.')));
assert.equal(store.load('/data/missing.json', null), null);
assert.equal(store.stats().writes, 1); assert.equal(store.stats().failures, 1);
assert.ok(store.stats().lastWriteMs >= 0);
console.log('PASS atomic encrypted commits, rollback on rename failure, corrupt-data preservation and timing metrics');
