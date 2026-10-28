'use strict';
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict');
const atomic=require('../atomic-json-store'),{createStorageCodec}=require('../storage-codec');
(async()=>{
    const dir=fs.mkdtempSync(path.join(os.tmpdir(),'ghost-transactions-'));
    const routes=new Map(),stores=[],key='isolated-integration-key-32-characters';
    let listening=false;
    const app={use(){},get(p,...h){routes.set('GET '+p,h)},post(p,...h){routes.set('POST '+p,h)},listen(_port,_host,ready){listening=true;ready();return{close:fn=>fn()}}};
    const express=Object.assign(()=>app,{json:()=>()=>{},urlencoded:()=>()=>{}});
    let hold=null,holdJournal=null,failJournal=false,failMessages=false,failAccounts=false;
    const c={Buffer,__dirname:dir,process:{env:{GHOST_SECRET:'fixture-secret',GHOST_STORAGE_KEY:key,DATA_DIR:dir},on(){},uptime:()=>1,pid:123},console:{log(){},warn(){},error(){}},setTimeout(){return{unref(){}}},setInterval(){},clearTimeout(){},clearInterval(){},fetch(){throw Error('Network forbidden')},require(name){
        if(name==='express')return express;if(name==='cors')return()=>()=>{};
        if(name==='dotenv')return{config(){}};if(name==='socket.io-client')return{io:()=>({on(){},onAny(){},removeAllListeners(){},disconnect(){}})};
        if(name==='./atomic-json-store')return{createStorageTransactions:atomic.createStorageTransactions,createAtomicJsonStore(k){
            const store=atomic.createAtomicJsonStore(k);stores.push(store);
            return{close:()=>store.close(),stats:()=>store.stats(),async write(file,value){
                if(failAccounts&&path.basename(file)==='accounts.json')throw Error('Injected identity write failure');
                if(path.basename(file)==='transaction.json'){
                    if(failJournal)throw Error('Injected journal write failure');
                    if(holdJournal){const gate=holdJournal;holdJournal=null;gate.started();await gate.wait;}
                }
                if(path.basename(file)==='messages.json'){
                    if(failMessages)throw Error('Injected disk write failure');
                    if(hold){const gate=hold;hold=null;gate.started();await gate.wait;}
                }
                return store.write(file,value);
            }};
        }};
        if(name.startsWith('./'))return require('../'+name.slice(2));
        return require(name);
    }};
    vm.createContext(c);vm.runInContext(fs.readFileSync(require.resolve('../server.js'),'utf8'),c);
    async function call(method,route,body={},query={}){
        let status=200,payload;
        const req={body,query,headers:{'x-ghost-secret':'fixture-secret','x-ghost-token':'fixture-token','x-ghost-user-id':'100','x-ghost-account-key':'100'}};
        const res={status(n){status=n;return this},json(v){payload=v;return this}};
        const handlers=routes.get(method+' '+route);
        async function dispatch(index){if(!handlers[index])return;let downstream;await handlers[index](req,res,()=>downstream=dispatch(index+1));if(downstream)await downstream;}
        await dispatch(0);return{status,payload};
    }
    try {
        await new Promise(resolve=>setImmediate(resolve));
        assert.equal(listening,true,'Server starts after asynchronous storage initialization');
        assert.equal((await call('POST','/api/session',{cookies:'user_id=100; PHPSESSID=fake',utk:'fixture-token',userId:'100'})).status,200);
        let started,release;const ready=new Promise(r=>started=r),wait=new Promise(r=>release=r);
        hold={started,wait};
        let acked=false;
        const first=call('POST','/api/messages/incoming',{peerId:'42',message:{id:'a',text:'one'}}).then(r=>{acked=true;return r});
        await ready;
        assert.equal(acked,false,'ACK cannot precede the message commit');
        assert.equal(vm.runInContext('messages.length',c),0,'Archive publishes only committed candidate');
        let healthTimer;
        try {
            const health=await Promise.race([call('GET','/api/health'),new Promise((_,reject)=>healthTimer=setTimeout(()=>reject(Error('Health endpoint blocked behind storage')),1000))]);
            assert.equal(health.status,200);assert.equal(health.payload.build,'relay-async-storage-v59-safe-20261028');
        } finally {clearTimeout(healthTimer);}
        const second=call('POST','/api/messages/incoming',{peerId:'42',message:{id:'b',text:'two'}});
        release();assert.equal((await first).status,200);assert.equal((await second).status,200);
        const pulled=(await call('GET','/api/sync',{}, {after_seq:'0'})).payload;
        assert.equal(pulled.messages.length,2);assert.equal(new Set(pulled.messages.map(m=>m.seq)).size,2);
        const codec=createStorageCodec(key),disk=codec.decode('messages.json',fs.readFileSync(path.join(dir,'messages.json'),'utf8'));
        assert.equal(disk.length,2,'Concurrent snapshots cannot overwrite a committed message');
        failMessages=true;
        assert.equal((await call('POST','/api/messages/incoming',{peerId:'42',message:{id:'c',text:'failed'}})).status,500);
        assert.equal(vm.runInContext('messages.length',c),2);
        assert.equal(codec.decode('messages.json',fs.readFileSync(path.join(dir,'messages.json'),'utf8')).length,2);
        failMessages=false;
        assert.equal((await call('POST','/api/messages/incoming',{peerId:'42',message:{id:'d',text:'after failure'}})).status,200);
        assert.equal((await call('GET','/api/sync',{}, {after_seq:'0'})).payload.messages.length,3);
        failAccounts=true;
        const enroll="getOwnerId({headers:{'x-ghost-secret':'fixture-secret','x-ghost-token':'new-alias','x-ghost-user-id':'100'}})";
        await assert.rejects(vm.runInContext(enroll,c),/identity persistence failed/);
        assert.equal(vm.runInContext("accountSessions['100'].utks.includes('new-alias')",c),false,'Failed identity writes cannot publish an uncommitted token');
        failAccounts=false;assert.equal(await vm.runInContext(enroll,c),'100');
        const status=await call('GET','/api/status');
        assert.ok(status.payload.storagePerformance.writes>0,'v59 storage metrics must include worker commits');
        assert.ok(status.payload.storagePerformance.maxWriteMs>=0);
        // A pending recovery journal must contain every accepted batch message
        // before the HTTP acknowledgement or the next transaction may proceed.
        const journalPath=path.join(dir,'transaction.json');
        fs.writeFileSync(journalPath,codec.encode('transaction.json',{id:'test',status:'pending',previous:{messages:[]}}));
        let journalStarted,releaseJournal;
        const journalReady=new Promise(r=>journalStarted=r),journalWait=new Promise(r=>releaseJournal=r);
        holdJournal={started:journalStarted,wait:journalWait};
        let batchAcked=false;
        const batch=call('POST','/api/messages/batch',{items:[
            {type:'incoming',peerId:'42',message:{id:'batch-one',text:'one'}},
            {type:'incoming',peerId:'42',message:{id:'batch-two',text:'two'}}
        ]}).then(r=>{batchAcked=true;return r});
        await journalReady;
        assert.equal(batchAcked,false,'Batch ACK must wait for pending journal update');
        releaseJournal();assert.equal((await batch).status,200);
        const journal=codec.decode('transaction.json',fs.readFileSync(journalPath,'utf8'));
        assert.deepEqual(journal.previous.messages.map(m=>m.id),['batch-one','batch-two']);
        failJournal=true;
        const rejected=await call('POST','/api/messages/batch',{items:[
            {type:'incoming',peerId:'42',message:{id:'batch-three',text:'three'}}
        ]});
        assert.equal(rejected.status,500,'Journal failure must not be acknowledged');
        assert.equal(vm.runInContext('fatalExitScheduled',c),true,'Journal failure requires recovery');
        assert.equal((await call('POST','/api/messages/incoming',{peerId:'42',message:{id:'blocked'}})).status,500,
            'Requests after fatal journal failure cannot mutate storage');
        const unhealthy=await call('GET','/api/health');
        assert.equal(unhealthy.payload.ok,false);
        assert.equal(unhealthy.payload.checks.storageReady,false);
        console.log('PASS real encrypted server commits: ACK ordering, concurrent message isolation, failure preserves disk/memory, subsequent writes recover. No external network.');
    } finally {for(const store of stores)await store.close();fs.rmSync(dir,{recursive:true,force:true});}
})().catch(error=>{console.error(error);process.exitCode=1;});
