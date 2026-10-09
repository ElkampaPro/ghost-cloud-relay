'use strict';
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const assert = require('node:assert/strict');

const root = path.resolve(__dirname, '..');
const src = fs.readFileSync(path.join(root, 'server.js'), 'utf8');

const routes = new Map();
const app = {
  use() {},
  get(p, ...h) { routes.set('GET ' + p, h); },
  post(p, ...h) { routes.set('POST ' + p, h); },
  listen() {}
};
const express = Object.assign(() => app, { json: () => () => {}, urlencoded: () => () => {} });

let socketDisconnects = 0;
let socketConnects = 0;

class FakeSocket {
  constructor() {
    this.connected = true;
    this.id = 'sock_' + Math.random().toString(36).slice(2);
    socketConnects++;
  }
  on() {}
  onAny() {}
  emit() {}
  removeAllListeners() {}
  disconnect() {
    this.connected = false;
    socketDisconnects++;
  }
}

const c = {
  Buffer,
  require(n) {
    if (n === 'crypto') return require('node:crypto');
    if (n === 'express') return express;
    if (n === 'cors') return () => () => {};
    if (n === 'dotenv') return { config() {} };
    if (n === 'path') return path;
    if (n === 'fs') return { existsSync: () => false, mkdirSync() {}, writeFileSync() {}, renameSync() {}, unlinkSync() {} };
    if (n === 'socket.io-client') return { io() { return new FakeSocket(); } };
    if (n === './storage-codec') return require('../storage-codec');
    if (n === './request-budget') return require('../request-budget');
    if (n === './cors-policy') return require('../cors-policy');
    throw Error(n);
  },
  __dirname: root,
  process: { env: { GHOST_SECRET: 'shared-test-secret' }, on() {}, pid: 1 },
  console: { log() {}, warn() {}, error() {} },
  setTimeout(fn) { if (typeof fn === 'function') fn(); },
  setInterval() {},
  clearInterval() {},
  fetch() { throw Error('network forbidden'); }
};

vm.createContext(c);
vm.runInContext(src, c);

function call(method, p, body = {}, query = {}, headers = {}) {
  let status = 200, payload;
  const req = {
    body,
    query,
    headers: { 'x-ghost-secret': 'shared-test-secret', ...headers }
  };
  const res = {
    status(v) { status = v; return this; },
    json(v) { payload = v; return this; }
  };
  const h = routes.get(method + ' ' + p);
  let i = 0;
  const next = () => h[i++]?.(req, res, next);
  next();
  return { status, payload };
}

console.log('=== TEST 1: Initial Session Registration ===');
const firstSync = call('POST', '/api/session', {
  cookies: 'user_id=100; PHPSESSID=session_alpha_123',
  utk: 'token_alpha',
  userId: '100',
  singleAccount: true
});
assert.equal(firstSync.status, 200);
assert.equal(firstSync.payload.ok, true);
assert.equal(firstSync.payload.changed, true, 'Initial sync must record change');
const initialDisconnects = socketDisconnects;

console.log('=== TEST 2: Repeated Identical Session Must Be Idempotent (changed: false) ===');
const repeatSync = call('POST', '/api/session', {
  cookies: 'user_id=100; PHPSESSID=session_alpha_123',
  utk: 'token_alpha',
  userId: '100',
  singleAccount: true
});
assert.equal(repeatSync.status, 200);
assert.equal(repeatSync.payload.ok, true);
assert.equal(repeatSync.payload.changed, false, 'Identical session resend must be idempotent (changed: false)');
assert.equal(socketDisconnects, initialDisconnects, 'Socket must NOT disconnect on identical session resend');

console.log('=== TEST 3: Shuffled Cookie Order Must Be Idempotent ===');
const shuffledSync = call('POST', '/api/session', {
  cookies: 'PHPSESSID=session_alpha_123; user_id=100;',
  utk: 'token_alpha',
  userId: '100',
  singleAccount: true
});
assert.equal(shuffledSync.status, 200);
assert.equal(shuffledSync.payload.ok, true);
assert.equal(shuffledSync.payload.changed, false, 'Shuffled cookie order must be idempotent');
assert.equal(socketDisconnects, initialDisconnects, 'Socket must NOT disconnect on shuffled cookie order');

console.log('=== TEST 4: Transient Cookie Additions With Same Auth Identity Must Not Reconnect Socket ===');
const transientSync = call('POST', '/api/session', {
  cookies: 'user_id=100; PHPSESSID=session_alpha_123; _cf_bm=new_transient_value; b_time=9999',
  utk: 'token_alpha',
  userId: '100',
  singleAccount: true
});
assert.equal(transientSync.status, 200);
assert.equal(transientSync.payload.ok, true);
assert.equal(transientSync.payload.changed, false, 'Transient cookie additions with identical PHPSESSID and UID must be idempotent');
assert.equal(socketDisconnects, initialDisconnects, 'Socket must NOT disconnect when auth session is identical');

console.log('=== TEST 5: Actual Credential Change (New PHPSESSID) Must Register Change ===');
const authChangeSync = call('POST', '/api/session', {
  cookies: 'user_id=100; PHPSESSID=session_beta_456',
  utk: 'token_alpha',
  userId: '100',
  singleAccount: true
});
assert.equal(authChangeSync.status, 200);
assert.equal(authChangeSync.payload.ok, true);
assert.equal(authChangeSync.payload.changed, true, 'Different PHPSESSID must register change');

console.log('=== ALL IDEMPOTENT SESSION SYNC TESTS PASSED! ===');
