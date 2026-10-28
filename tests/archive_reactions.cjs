(async () => {
'use strict';
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const {pmMetadata}=require('../pm-metadata');
const source=fs.readFileSync(require.resolve('../server.js'),'utf8');
const fn=source.slice(source.indexOf('async function persistPrivateReaction('),source.indexOf('\nfunction initChatSocket()',source.indexOf('async function persistPrivateReaction(')));
let writes=0,seq=8,fail=false;
const ctx={pmMetadata,JSON,Number,String,messages:[{owner:'100',peerId:'42',id:'501',html:'captured media',text:'original',seq:8,read:false}],MESSAGES_FILE:'fixture',nextOwnerSeq:()=>++seq,saveJsonAsync(){writes++;return !fail;},recordIncomingToPendingTransaction(){}};
vm.createContext(ctx);vm.runInContext(fn,ctx);
const react={v:2,id:'501',peer:'42',by:'42',key:'1f496',list:[['1f496',1]]};
assert.equal((await ctx.persistPrivateReaction('200',react)),false,'Different owner cannot change archive');
assert.equal((await ctx.persistPrivateReaction('100',{...react,peer:'99'})),false,'Different peer rejected');
assert.equal((await ctx.persistPrivateReaction('100',react)),true);
assert.equal(ctx.messages[0].html,'captured media');assert.equal(ctx.messages[0].text,'original');assert.equal(ctx.messages[0].read,false);
assert.equal(ctx.messages[0].seq,9,'Metadata gets a delta delivery sequence');assert.equal(ctx.messages[0].reactionState.total,1);
assert.equal((await ctx.persistPrivateReaction('100',react)),true);assert.equal(writes,1,'Repeated reaction does not write or advance sequence');
fail=true;assert.equal((await ctx.persistPrivateReaction('100',{...react,list:[]})),false);assert.equal(ctx.messages[0].reactionState.total,1,'Failed save leaves committed archive unchanged');
assert.deepEqual(pmMetadata({sourceDeleted:true,deleted:true,html:'erased',read:true,lrid:9}),{sourceDeleted:true},'Deletion metadata never erases content or copies read state');
console.log('PASS: archive retained, reaction deltas isolated/idempotent, failed writes safe, no read state');

})().catch(error=>{console.error(error);process.exitCode=1;});
