(async () => {
const fs=require('fs'),vm=require('vm'),assert=require('node:assert/strict');
const source=fs.readFileSync(require.resolve('../server.js'),'utf8');
const fn=source.slice(source.indexOf('async function persistPrivateDeletion('),source.indexOf('async function persistPrivateReaction('));
let writes=0,seq=8,fail=false;
const ctx={messages:[{owner:'100',peerId:'42',id:'501',type:'received',html:'original',read:false,reactionState:{total:1},seq:8}],MESSAGES_FILE:'fixture',nextOwnerSeq:()=>++seq,saveJsonAsync(){writes++;return !fail;},recordIncomingToPendingTransaction(){}};
vm.createContext(ctx);vm.runInContext(fn,ctx);const data={id:'501',peer:'42',scope:'all',v:2};
assert.equal((await ctx.persistPrivateDeletion('200',data)),false);assert.equal((await ctx.persistPrivateDeletion('100',{...data,scope:'me'})),false);
fail=true;assert.equal((await ctx.persistPrivateDeletion('100',data)),false);assert.equal(ctx.messages[0].sourceDeleted,undefined);
fail=false;assert.equal((await ctx.persistPrivateDeletion('100',data)),true);assert.equal(ctx.messages[0].html,'original');assert.equal(ctx.messages[0].read,false);assert.equal(ctx.messages[0].reactionState.total,1);assert.equal(ctx.messages[0].sourceDeletedBySender,true);
const committedSeq=seq,committedWrites=writes;assert.equal((await ctx.persistPrivateDeletion('100',data)),true);assert.equal(seq,committedSeq);assert.equal(writes,committedWrites);
ctx.messages[0].type='sent';assert.equal((await ctx.persistPrivateDeletion('100',data)),false);
console.log('PASS sender deletion metadata isolated, durable, idempotent; original content/read/reactions preserved');

})().catch(error=>{console.error(error);process.exitCode=1;});
