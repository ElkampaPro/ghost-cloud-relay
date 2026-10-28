'use strict';
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),assert=require('node:assert/strict');
const {createAtomicJsonStore,createStorageTransactions}=require('../atomic-json-store');
const {createStorageCodec}=require('../storage-codec');
(async()=>{
    const dir=fs.mkdtempSync(path.join(os.tmpdir(),'ghost-atomic-'));
    const key='isolated-test-key-32-characters-long';
    const store=createAtomicJsonStore(key),queue=createStorageTransactions();
    try {
        const file=path.join(dir,'messages.json'),codec=createStorageCodec(key);
        const one={rows:[{id:'1',text:'snapshot'}]};
        const first=store.write(file,one);one.rows[0].text='mutated after submission';
        await first;
        assert.deepEqual(codec.decode('messages.json',fs.readFileSync(file,'utf8')),{rows:[{id:'1',text:'snapshot'}]});
        assert(!fs.readFileSync(file,'utf8').includes('snapshot'));
        await Promise.all([store.write(file,{revision:2}),store.write(file,{revision:3})]);
        assert.equal(codec.decode('messages.json',fs.readFileSync(file,'utf8')).revision,3);
        const blocked=path.join(dir,'directory');fs.mkdirSync(blocked);
        await assert.rejects(store.write(blocked,{will:'fail'}));
        await store.write(file,{revision:4});
        assert.equal(codec.decode('messages.json',fs.readFileSync(file,'utf8')).revision,4,'A failed write cannot poison the queue');
        assert.equal(store.stats().writes,4);
        assert.equal(store.stats().failures,1);
        assert.ok(store.stats().maxWriteMs>=store.stats().lastWriteMs);
        assert(!fs.readdirSync(dir).some(name=>name.startsWith('.tmp_')),'Failed write removes its temporary file');
        let release;const wait=new Promise(resolve=>release=resolve),events=[];
        let inherited,queued;
        const callbackQueued=new Promise(resolve=>queued=resolve);
        const a=queue.run(async()=>{
            events.push('a');
            // A detached callback born inside a live transaction must wait,
            // rather than being mistaken for an authorized reentrant helper.
            inherited=new Promise(resolve=>setImmediate(()=>{const task=queue.run(()=>events.push('detached'));queued();resolve(task);}));
            await wait;events.push('commit-a');
        });
        const b=queue.run(()=>events.push('b'));
        await callbackQueued;
        assert.deepEqual(events,['a']);
        let closed=false;const drain=queue.close().then(()=>closed=true);
        await assert.rejects(queue.run(()=>{}),/shutting down/);
        assert.equal(closed,false);release();await Promise.all([a,b,drain,inherited]);
        assert.deepEqual(events,['a','commit-a','b','detached']);
        await store.close();await assert.rejects(store.write(file,{}),/shutting down/);
        console.log('PASS real atomic/encrypted writes, snapshot isolation, write failure/recovery, temp cleanup, FIFO including inherited callbacks, shutdown drain.');
    } finally {
        await store.close();
        fs.rmSync(dir,{recursive:true,force:true});
    }
})().catch(error=>{console.error(error);process.exitCode=1;});
