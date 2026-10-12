'use strict';
// Only presentation metadata. Read/ACK cursors and flags are never accepted.
function pmMetadata(message) {
    const out={};
    if(['text','image','video','voice','music','file','youtube'].includes(message?.kind))out.kind=message.kind;
    if(/^\d{1,20}$/.test(String(message?.replyTo||''))){
        out.replyTo=String(message.replyTo);
        out.replyName=String(message.replyName||'').slice(0,120);
        out.replyPreview=String(message.replyPreview||'').slice(0,120);
        out.replyOwn=message.replyOwn===true;
        if(['text','image','video','voice','music','file','youtube'].includes(message.replyKind))out.replyKind=message.replyKind;
    }
    if(typeof message?.canReact==='boolean')out.canReact=message.canReact;
    if(message?.reactionState&&Array.isArray(message.reactionState.list)) {
        const list=message.reactionState.list.filter(x=>Array.isArray(x)&&/^[a-z0-9-]{1,64}$/i.test(String(x[0]))&&Number.isSafeInteger(Number(x[1]))&&Number(x[1])>0).slice(0,100).map(x=>[String(x[0]),Number(x[1])]);
        out.reactionState={list,total:list.reduce((n,x)=>n+x[1],0),mine:typeof message.reactionState.mine==='string'&&/^[a-z0-9-]{1,64}$/i.test(message.reactionState.mine)?message.reactionState.mine:null};
    }
    if(message?.sourceDeleted===true)out.sourceDeleted=true;
    return out;
}
function nativePmBody(html) {
    const value=String(html||'');
    const start=/<div\b[^>]*\bclass=["'][^"']*\bpm_body\b[^"']*["'][^>]*>/i.exec(value);
    if(!start)return null;
    const begin=start.index+start[0].length, tags=/<\/?div\b[^>]*>/gi;
    tags.lastIndex=begin;let depth=1,match;
    while((match=tags.exec(value))){depth+=/^<\//.test(match[0])?-1:1;if(!depth)return value.slice(begin,match.index);}
    return null;
}
module.exports={pmMetadata,nativePmBody};
