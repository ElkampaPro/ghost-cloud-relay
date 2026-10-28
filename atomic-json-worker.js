'use strict';
const { parentPort, workerData } = require('node:worker_threads');
const fs = require('node:fs/promises');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { createStorageCodec } = require('./storage-codec');
const codec = createStorageCodec(workerData.encodedKey);
let tail = Promise.resolve();
let uncertainFailure = null;
parentPort.on('message', request => {
    const task = tail.then(async () => {
        if (uncertainFailure) {
            parentPort.postMessage({ id: request.id, ok: false, code: 'COMMIT_UNCERTAIN', error: uncertainFailure });
            return;
        }
        let temporary, handle, renamed = false;
        try {
            const file = path.resolve(request.file), directory = path.dirname(file);
            await fs.mkdir(directory, { recursive: true });
            const encoded = codec.encode(path.basename(file), request.value);
            temporary = path.join(directory, `.tmp_${path.basename(file)}_${randomUUID()}`);
            handle = await fs.open(temporary, 'wx', 0o600);
            await handle.writeFile(encoded, 'utf8');
            await handle.sync();
            await handle.close();
            handle = null;
            await fs.rename(temporary, file);
            renamed = true;
            temporary = null;
            // Persist the directory entry on platforms that support it.
            if (process.platform !== 'win32') {
                const dir = await fs.open(directory, 'r');
                try { await dir.sync(); } finally { await dir.close(); }
            }
            parentPort.postMessage({ id: request.id, ok: true });
        } catch (error) {
            if (handle) await handle.close().catch(() => {});
            if (temporary) await fs.unlink(temporary).catch(() => {});
            if (renamed) uncertainFailure = 'File replaced but directory durability could not be confirmed';
            parentPort.postMessage({ id: request.id, ok: false,
                code: renamed ? 'COMMIT_UNCERTAIN' : error.code,
                error: renamed ? uncertainFailure : error.message });
        }
    });
    tail = task.catch(() => {});
});
