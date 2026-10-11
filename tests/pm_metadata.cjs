const assert=require('node:assert/strict'),{pmMetadata,nativePmBody}=require('../pm-metadata');
assert.deepEqual(pmMetadata({kind:'voice',replyTo:123,replyName:'A',replyPreview:'B',replyOwn:true,canReact:false,read:true,lrid:999}),{kind:'voice',replyTo:'123',replyName:'A',replyPreview:'B',replyOwn:true,canReact:false});
assert.deepEqual(pmMetadata({kind:'bad',replyTo:'bad',canReact:'true',readThrough:10}),{});
assert.equal(pmMetadata({replyTo:5,replyPreview:'x'.repeat(500)}).replyPreview.length,120);
assert.equal(nativePmBody('<li class="pm_msg"><div class="pm_body">text<div class="pm_audio">media</div></div><span class="pm_time">12:00</span></li>'),'text<div class="pm_audio">media</div>');
assert.equal(nativePmBody('<div class="other">text</div>'),null);
console.log('PASS PM presentation metadata; no read state copied');
