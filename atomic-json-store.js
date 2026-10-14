'use strict';

// One storage boundary for encrypted, atomic snapshots. Keep synchronous commit
// semantics: callers update live state/ACK only after the rename has succeeded.
function createAtomicJsonStore({ fs, path, codec, logger = console, now = () => performance.now() }) {
    const metrics = { writes: 0, failures: 0, lastWriteMs: 0, maxWriteMs: 0 };
    function load(file, fallback) {
        if (!fs.existsSync(file)) return fallback;
        try {
            return codec.decode(path.basename(file), fs.readFileSync(file, 'utf8'));
        } catch (error) {
            logger.error('[Storage] Cannot authenticate/parse snapshot; preserving original:', error.message);
            try { fs.copyFileSync(file, `${file}.corrupt.${Date.now()}`); } catch (_) {}
            throw Error('Stored data could not be authenticated or parsed; startup aborted');
        }
    }
    function save(file, data) {
        let temporary = null;
        const started = now();
        try {
            const directory = path.dirname(file);
            if (!fs.existsSync(directory)) fs.mkdirSync(directory, { recursive: true });
            temporary = path.join(directory, `.tmp_${path.basename(file)}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`);
            fs.writeFileSync(temporary, codec.encode(path.basename(file), data), { encoding: 'utf8', mode: 0o600 });
            fs.renameSync(temporary, file);
            metrics.writes++;
            return true;
        } catch (error) {
            metrics.failures++;
            if (temporary) { try { if (fs.existsSync(temporary)) fs.unlinkSync(temporary); } catch (_) {} }
            logger.error('[Storage] Snapshot commit failed:', error.message);
            return false;
        } finally {
            metrics.lastWriteMs = Math.max(0, now() - started);
            metrics.maxWriteMs = Math.max(metrics.maxWriteMs, metrics.lastWriteMs);
        }
    }
    return Object.freeze({ load, save, stats: () => ({ ...metrics }) });
}

module.exports = { createAtomicJsonStore };
