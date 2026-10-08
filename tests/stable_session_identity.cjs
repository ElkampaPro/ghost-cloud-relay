// Regression test suite for S01 (Concurrent Client Isolation) and S02 (UTK Rotation Persistence).
// PASS means defects are properly FIXED and verified.
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

const c = {Buffer,
  require(n) {
    if (n === 'crypto') return require('node:crypto');
    if (n === 'express') return express;
    if (n === 'cors') return () => () => {};
    if (n === 'dotenv') return { config() {} };
    if (n === 'path') return path;
    if (n === 'fs') return { existsSync: () => false, mkdirSync() {}, writeFileSync() {}, renameSync() {}, unlinkSync() {} };
    if (n === 'socket.io-client') return { io() { return { on() {}, onAny() {}, removeAllListeners() {}, disconnect() {} }; } };
    if (n === './storage-codec') return require('../storage-codec');
    if (n === './request-budget') return require('../request-budget');
    throw Error(n);
  },
  __dirname: root,
  process: { env: { GHOST_SECRET: 'shared-test-secret' }, on() {}, pid: 1 },
  console: { log() {}, warn() {}, error() {} },
  setTimeout() {},
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


const server=c;
function session(token,cookie,userId='100'){return call('POST','/api/session',{cookies:'PHPSESSID='+cookie,utk:token,userId,singleAccount:true})}
function headers(token,id='100'){return {'x-ghost-token':token,'x-ghost-user-id':id,'x-ghost-account-key':id}}
assert.equal(session('first','cookie-a').status,200);
assert.equal(call('POST','/api/messages/incoming',{peerId:'p',message:{id:'msg1',text:'fixture'}},{},headers('first')).status,200);
let state=vm.runInContext('({keys:Object.keys(accountSessions).filter(k=>!accountSessions[k].revoked),sessionId:sessionData.userId})',server);
assert.equal(state.keys.length,1,'Read requests cannot create a second account from PHP session');
assert.equal(state.keys[0],'100');
assert.equal(state.sessionId,'100','Explicit approved identity survives session persistence');
assert.equal(session('second','cookie-b').status,200);
call('POST','/api/messages/incoming',{peerId:'p',message:{id:'msg2',text:'fixture2'}},{},headers('second'));
const a=call('GET','/api/sync',{}, {all:'true',after_seq:'0'},headers('first')).payload;
const b=call('GET','/api/sync',{}, {all:'true',after_seq:'0'},headers('second')).payload;
assert.equal(a.messages.length,2);assert.equal(b.messages.length,2);
assert.ok(b.current_seq>=Math.max(...b.messages.map(m=>m.seq)),'Counter must cover all retained message sequences');
assert.equal(a.current_seq,b.current_seq);
// A legacy owner has evidence for the same identity; renewal migrates its sequence horizon.
vm.runInContext(`accountSessions={'phpsess_legacy':{utk:'legacy',utks:['legacy'],cookies:'PHPSESSID=old',revoked:false}};sessionData={utk:'legacy',cookies:'PHPSESSID=old'};messages=[{owner:'phpsess_legacy',peerId:'p',id:'legacy-id',type:'received',timestamp:1,seq:90}];ownerSeqCounters={'phpsess_legacy':90};`,server);
assert.equal(session('legacy','old').status,200);
assert.equal(vm.runInContext('sessionData.accountKey',server),'100','Legacy enrollment upgrades identity even when cookies/token are unchanged');
const migrated=call('GET','/api/sync',{}, {all:'true',after_seq:'0'},headers('legacy')).payload;
assert.equal(migrated.messages.length,1);assert.ok(migrated.current_seq>=90,'Migration must preserve the delivery sequence horizon');
console.log('Stable cloud identity across device/session rotation and sequence migration passed');
