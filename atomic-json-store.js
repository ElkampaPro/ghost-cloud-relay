'use strict';
const { Worker } = require('node:worker_threads');
const path = require('node:path');

// Serialize the whole read/modify/commit operation, not just its disk write.
// Otherwise two requests can build competing snapshots from the same state.
function createStorageTransactions() {
    let tail = Promise.resolve();
    let closing = false;
    function run(task) {
        if (closing) return Promise.reject(Error('Storage is shutting down'));
        // Always queue event entry points, including callbacks created during
        // another transaction. Ambient async context must never bypass FIFO.
        // Internal helpers are called directly, without acquiring this gate.
        const operation = tail.then(task);
        tail = operation.catch(() => {});
        return operation;
    }
    return { run, drain: () => tail, async close() { closing = true; await tail; } };
}

// JSON encoding, encryption and file I/O happen outside the HTTP event loop.
// Each caller resolves only after its own atomic write has committed.
function createAtomicJsonStore(encodedKey = '') {
    const worker = new Worker(path.join(__dirname, 'atomic-json-worker.js'), { workerData: { encodedKey } });
    const pending = new Map();
    let nextId = 0, failed = null, closing = false;
    const metrics = { writes: 0, failures: 0, lastWriteMs: 0, maxWriteMs: 0 };
    worker.unref();
    function fail(error) {
        failed = error;
        for (const request of pending.values()) request.reject(error);
        pending.clear();
        worker.unref();
    }
    function workerFailure(error) {
        // A worker that disappears with a write in flight may have renamed the
        // file without reporting its result. Never assume the old disk state.
        if (pending.size) error.code = 'COMMIT_UNCERTAIN';
        fail(error);
    }
    worker.on('error', workerFailure);
    worker.on('exit', code => { if (!closing || pending.size) workerFailure(Error(`Storage worker exited (${code})`)); });
    worker.on('message', message => {
        const request = pending.get(message.id);
        if (!request) return;
        pending.delete(message.id);
        if (!pending.size) worker.unref();
        if (message.ok) request.resolve(true);
        else {
            const error = Object.assign(Error(message.error || 'Atomic write failed'), { code: message.code });
            request.reject(error);
            if (message.code === 'COMMIT_UNCERTAIN') fail(error);
        }
    });
    return {
        write(file, value) {
            const started = Date.now();
            const request = failed || closing ? Promise.reject(failed || Error('Storage is shutting down')) : new Promise((resolve, reject) => {
                const id = ++nextId;
                pending.set(id, { resolve, reject });
                worker.ref();
                // postMessage snapshots the value before this method returns.
                try { worker.postMessage({ id, file, value }); }
                catch (error) {
                    pending.delete(id);
                    if (!pending.size) worker.unref();
                    reject(error);
                }
            });
            return request.then(result => { metrics.writes++; return result; }, error => {
                metrics.failures++; throw error;
            }).finally(() => {
                metrics.lastWriteMs = Math.max(0, Date.now() - started);
                metrics.maxWriteMs = Math.max(metrics.maxWriteMs, metrics.lastWriteMs);
            });
        },
        stats() { return { ...metrics }; },
        async close() {
            closing = true;
            // All production callers close the transaction queue first.
            while (pending.size) await new Promise(resolve => setTimeout(resolve, 5));
            await worker.terminate();
        }
    };
}
module.exports = { createAtomicJsonStore, createStorageTransactions };
