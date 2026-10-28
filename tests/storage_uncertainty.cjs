'use strict';
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const source=fs.readFileSync(require.resolve('../atomic-json-worker.js'),'utf8');

async function runScenario(failAt) {
    let handler, response=[], disk='original', temp='', renameCount=0;
    const fileSystem={
        async mkdir(){},
        async open(_path,mode){
            if(mode==='wx') return {
                async writeFile(value){ if(failAt==='write') throw Error('injected write failure');temp=value; },
                async sync(){},async close(){}
            };
            return {async sync(){if(failAt==='directory') throw Error('injected directory sync failure');},async close(){}};
        },
        async rename(){disk=temp;renameCount++;},async unlink(){}
    };
    const parentPort={on(_event,fn){handler=fn;},postMessage(value){response.push(value);}};
    const context={process:{platform:'linux'},require(name){
        if(name==='node:worker_threads')return{parentPort,workerData:{encodedKey:''}};
        if(name==='node:fs/promises')return fileSystem;
        if(name==='node:path')return require('node:path');
        if(name==='node:crypto')return{randomUUID:()=> 'test'};
        if(name==='./storage-codec')return{createStorageCodec:()=>({encode:(_name,value)=>JSON.stringify(value)})};
        throw Error(name);
    }};
    vm.runInNewContext(source,context);
    handler({id:1,file:'/test/messages.json',value:{value:'first'}});
    handler({id:2,file:'/test/messages.json',value:{value:'second'}});
    while(response.length<2) await new Promise(resolve=>setImmediate(resolve));
    return{response,disk,renameCount};
}

async function testWorkerExitWithPendingWrite() {
    const {EventEmitter}=require('node:events');
    let instance;
    class FakeWorker extends EventEmitter {
        constructor(){super();instance=this;}
        ref(){} unref(){} postMessage(){} async terminate(){}
    }
    const sandbox={__dirname:'.',module:{exports:{}},require(name){
        if(name==='node:worker_threads')return{Worker:FakeWorker};
        if(name==='node:path')return require('node:path');
        throw Error(name);
    }};
    vm.runInNewContext(fs.readFileSync(require.resolve('../atomic-json-store.js'),'utf8'),sandbox);
    const store=sandbox.module.exports.createAtomicJsonStore('');
    const write=store.write('/test/messages.json',{value:1});
    instance.emit('exit',1);
    await assert.rejects(write,error=>error.code==='COMMIT_UNCERTAIN');
    await assert.rejects(store.write('/test/messages.json',{value:2}));
}

async function testServerStopsOnUncertainty() {
    const text=fs.readFileSync(require.resolve('../server.js'),'utf8');
    const section=text.slice(text.indexOf('async function saveJsonAsync('),text.indexOf('function storageRoute('));
    let halted=false;
    const sandbox={fatalExitScheduled:false,console:{error(){}},process:{env:{}},
        runtimeStorage:{async write(){throw Object.assign(Error('ambiguous'),{code:'COMMIT_UNCERTAIN'});}},
        scheduleFatalExit(){halted=true;sandbox.fatalExitScheduled=true;}};
    vm.runInNewContext(section,sandbox);
    await assert.rejects(sandbox.saveJsonAsync('/test/messages.json',{}),error=>error.code==='COMMIT_UNCERTAIN');
    assert.equal(halted,true);
    await assert.rejects(sandbox.saveJsonAsync('/test/messages.json',{}),error=>error.code==='COMMIT_UNCERTAIN');
}

(async()=>{
    const uncertain=await runScenario('directory');
    assert.equal(uncertain.response[0].code,'COMMIT_UNCERTAIN');
    assert.equal(uncertain.response[1].code,'COMMIT_UNCERTAIN');
    assert.equal(JSON.parse(uncertain.disk).value,'first');
    assert.equal(uncertain.renameCount,1,'Uncertain outcome blocks subsequent queued writes');
    const beforeRename=await runScenario('write');
    assert.equal(beforeRename.response[0].ok,false);
    assert.notEqual(beforeRename.response[0].code,'COMMIT_UNCERTAIN');
    assert.equal(beforeRename.disk,'original');
    await testWorkerExitWithPendingWrite();
    await testServerStopsOnUncertainty();
    console.log('PASS: post-rename uncertainty blocks subsequent writes; pre-rename failure preserves old file.');
})().catch(error=>{console.error(error);process.exitCode=1;});
