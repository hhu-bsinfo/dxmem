package de.hhu.bsinfo.dxmem.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * LockManager handling locking for chunks in CIDTable
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2018
 */
public final class LockManager {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LockManager.class.getSimpleName());

    private static final ValuePool SOP_READ_LOCK_REQS = new ValuePool(DXMem.class, "ReadLockReqs");
    private static final ValuePool SOP_READ_LOCK_RETRIES = new ValuePool(DXMem.class, "ReadLockRetries");
    private static final ValuePool SOP_READ_LOCK_TIMEOUTS = new ValuePool(DXMem.class, "ReadLockTimeouts");
    private static final ValuePool SOP_WRITE_LOCK_REQS = new ValuePool(DXMem.class, "WriteLockReqs");
    private static final ValuePool SOP_WRITE_LOCK_RETRIES = new ValuePool(DXMem.class, "WriteLockRetries");
    private static final ValuePool SOP_WRITE_LOCK_TIMEOUTS = new ValuePool(DXMem.class, "WriteLockTimeouts");
    private static final ValuePool SOP_SWAP_WRITE_LOCK_REQS = new ValuePool(DXMem.class, "SwapWriteLockReqs");
    private static final ValuePool SOP_SWAP_WRITE_LOCK_RETRIES = new ValuePool(DXMem.class, "SwapWriteLockRetries");
    private static final ValuePool SOP_SWAP_WRITE_LOCK_TIMEOUTS = new ValuePool(DXMem.class, "SwapWriteLockTimeouts");
    private static final ValuePool SOP_SWAP_READ_LOCK_REQS = new ValuePool(DXMem.class, "SwapReadLockReqs");
    private static final ValuePool SOP_SWAP_READ_LOCK_RETRIES = new ValuePool(DXMem.class, "SwapReadLockRetries");
    private static final ValuePool SOP_SWAP_READ_LOCK_TIMEOUTS = new ValuePool(DXMem.class, "SwapReadLockTimeouts");

    static {
        StatisticsManager.get().registerOperation(DXMem.class, SOP_READ_LOCK_REQS);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_READ_LOCK_RETRIES);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_READ_LOCK_TIMEOUTS);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_WRITE_LOCK_REQS);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_WRITE_LOCK_RETRIES);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_WRITE_LOCK_TIMEOUTS);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_SWAP_WRITE_LOCK_REQS);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_SWAP_WRITE_LOCK_RETRIES);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_SWAP_WRITE_LOCK_TIMEOUTS);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_SWAP_READ_LOCK_REQS);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_SWAP_READ_LOCK_RETRIES);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_SWAP_READ_LOCK_TIMEOUTS);
    }

    /**
     * Return value/status for lock operation
     */
    public enum LockStatus {
        OK,
        INVALID,
        TIMEOUT
    }

    /**
     * Static class
     */
    private LockManager() {

    }

    /**
     * Execute a lock operation BEFORE the memory operation
     *
     * @param p_cidTable
     *         CIDTable instance
     * @param p_entry
     *         Chunk entry of CIDTable to lock/unlock
     * @param p_op
     *         Lock operation to execute (see enum)
     * @param p_lockTimeoutMs
     *         -1 = infinite, 0 = one shot, &gt; 0 timeout in ms
     * @return Lock status
     */
    public static LockStatus executeBeforeOp(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry,
            final ChunkLockOperation p_op, final int p_lockTimeoutMs) {
        // note: this should be turned into some sort of jumptable by the JIT making it not as expensive as it looks
        switch (p_op) {
            case NONE:
            case WRITE_LOCK_ACQ_POST_OP:
            case WRITE_LOCK_REL_POST_OP:
            case WRITE_LOCK_SWAP_POST_OP:
            case READ_LOCK_ACQ_POST_OP:
            case READ_LOCK_REL_POST_OP:
            case READ_LOCK_SWAP_POST_OP:
                return LockStatus.OK;

            case WRITE_LOCK_ACQ_PRE_OP:
            case WRITE_LOCK_ACQ_OP_REL:
            case WRITE_LOCK_ACQ_OP_SWAP:
                return acquireWriteLock(p_cidTable, p_entry, p_lockTimeoutMs);

            case WRITE_LOCK_SWAP_PRE_OP:
            case WRITE_LOCK_SWAP_OP_REL:
                return swapWriteLockForReadLock(p_cidTable, p_entry, p_lockTimeoutMs);

            case READ_LOCK_ACQ_PRE_OP:
            case READ_LOCK_ACQ_OP_REL:
            case READ_LOCK_ACQ_OP_SWAP:
                return acquireReadLock(p_cidTable, p_entry, p_lockTimeoutMs);

            case READ_LOCK_SWAP_PRE_OP:
            case READ_LOCK_SWAP_OP_REL:
                return swapReadLockForWriteLock(p_cidTable, p_entry, p_lockTimeoutMs);

            default:
                throw new IllegalStateException("Unhandled lock operation");
        }
    }

    /**
     * Execute a lock operation AFTER the memory operation
     *
     * @param p_cidTable
     *         CIDTable instance
     * @param p_entry
     *         Chunk entry of CIDTable to lock/unlock
     * @param p_op
     *         Lock operation to execute (see enum)
     * @param p_lockTimeoutMs
     *         -1 = infinite, 0 = one shot, &gt; 0 timeout in ms
     * @return Lock status
     */
    public static LockStatus executeAfterOp(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry,
            final ChunkLockOperation p_op, final int p_lockTimeoutMs) {
        // note: this should be turned into some sort of jumptable by the JIT making it not as expensive as it looks
        switch (p_op) {
            case NONE:
            case WRITE_LOCK_ACQ_PRE_OP:
            case WRITE_LOCK_SWAP_PRE_OP:
            case READ_LOCK_ACQ_PRE_OP:
            case READ_LOCK_SWAP_PRE_OP:
                return LockStatus.OK;

            case WRITE_LOCK_ACQ_POST_OP:
                return acquireWriteLock(p_cidTable, p_entry, p_lockTimeoutMs);

            case WRITE_LOCK_REL_POST_OP:
            case WRITE_LOCK_ACQ_OP_REL:
            case WRITE_LOCK_SWAP_OP_REL:
                releaseWriteLock(p_cidTable, p_entry);
                return LockStatus.OK;

            case WRITE_LOCK_SWAP_POST_OP:
            case WRITE_LOCK_ACQ_OP_SWAP:
                return swapWriteLockForReadLock(p_cidTable, p_entry, p_lockTimeoutMs);

            case READ_LOCK_ACQ_POST_OP:
                return acquireReadLock(p_cidTable, p_entry, p_lockTimeoutMs);

            case READ_LOCK_REL_POST_OP:
            case READ_LOCK_ACQ_OP_REL:
            case READ_LOCK_SWAP_OP_REL:
                releaseReadLock(p_cidTable, p_entry);
                return LockStatus.OK;

            case READ_LOCK_SWAP_POST_OP:
            case READ_LOCK_ACQ_OP_SWAP:
                return swapReadLockForWriteLock(p_cidTable, p_entry, p_lockTimeoutMs);

            default:
                throw new IllegalStateException("Unhandled lock operation");
        }
    }

    /**
     * Acquire a read lock to a chunk
     * This call might also re-read the entry if acquiring the lock did not succeed on the first try
     * if the lock was acquired successfully, the (re-read) entry is guaranteed to be valid
     *
     * @param p_cidTable
     *         CIDTable instance
     * @param p_entry
     *         Chunk entry of CIDTable to lock
     * @param p_retryTimeoutMs
     *         -1 = infinite, 0 = one shot, &gt; 0 timeout in ms
     * @return Lock status
     */
    private static LockStatus acquireReadLock(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry,
            final int p_retryTimeoutMs) {
        long startTime = 0;

        if (p_retryTimeoutMs > 0) {
            startTime = System.nanoTime();
        }

        SOP_READ_LOCK_REQS.inc();

        int retries = 0;

        p_entry.currentStateInitialState();

        while (true) {
            // entry turned invalid, e.g. chunk was deleted
            if (!p_entry.isValid()) {
                return LockStatus.INVALID;
            }

            if (!p_entry.isWriteLockAcquired()) {
                if (p_entry.acquireReadLock()) {
                    // read lock acquired, try to persist state
                    if (p_cidTable.entryAtomicUpdate(p_entry)) {
                        if (retries > 0) {
                            SOP_READ_LOCK_RETRIES.add(retries);
                        }

                        return LockStatus.OK;
                    }
                } else {
                    // log once to avoid flooding the log on retries
                    if (retries == 0) {
                        LOGGER.warn("Max number of read locks already acquired for %s", p_entry);
                    }
                }
                // else: no locks available because too many readers active, we have to wait
            }
            // else: write lock acquired, don't enter until released

            if (p_retryTimeoutMs >= 0) {
                if (p_retryTimeoutMs == 0 || System.nanoTime() - startTime >= p_retryTimeoutMs * 1000 * 1000) {
                    // return with current state
                    p_cidTable.entryReread(p_entry);
                    SOP_READ_LOCK_TIMEOUTS.inc();
                    return LockStatus.TIMEOUT;
                }
            }

            Thread.yield();
            retries++;

            p_cidTable.entryReread(p_entry);
        }
    }

    /**
     * Release an acquired read lock
     *
     * @param p_cidTable
     *         CIDTable instance
     * @param p_entry
     *         Chunk entry of CIDTable to unlock
     */
    private static void releaseReadLock(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry) {
        p_entry.currentStateInitialState();

        while (true) {
            // invalid state, chunk was deleted but a lock was still acquired
            if (!p_entry.isValid()) {
                throw new IllegalStateException("Release read lock of deleted chunk " + p_entry);
            }

            // write lock might be acquired: writer thread blocks all further reader threads and waits for
            // current readers in critical section to exit
            if (!p_entry.areReadLocksAcquired()) {
                throw new IllegalStateException("Releasing read lock with no read locks acquired for " + p_entry);
            }

            p_entry.releaseReadLock();

            // read lock released, try to persist state
            if (p_cidTable.entryAtomicUpdate(p_entry)) {
                break;
            }

            Thread.yield();
            p_cidTable.entryReread(p_entry);
        }
    }

    /**
     * Acquire a write lock to a chunk
     * This call might also re-read the entry if acquiring the lock did not succeed on the first try
     * if the lock was acquired successfully, the (re-read) entry is guaranteed to be valid
     *
     * @param p_cidTable
     *         CIDTable instance
     * @param p_entry
     *         Chunk entry of CIDTable to lock
     * @param p_retryTimeoutMs
     *         -1 = infinite, 0 = one shot, &gt; 0 timeout in ms
     * @return Lock status
     */
    private static LockStatus acquireWriteLock(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry,
            final int p_retryTimeoutMs) {
        long startTime = 0;

        if (p_retryTimeoutMs > 0) {
            startTime = System.nanoTime();
        }

        SOP_WRITE_LOCK_REQS.inc();

        int retries = 0;

        p_entry.currentStateInitialState();

        while (true) {
            // entry turned invalid, e.g. chunk was deleted
            if (!p_entry.isValid()) {
                return LockStatus.INVALID;
            }

            if (p_entry.acquireWriteLock()) {
                // try to persist state
                if (p_cidTable.entryAtomicUpdate(p_entry)) {
                    // now, wait for all readers to exit the section

                    while (p_entry.areReadLocksAcquired()) {
                        if (p_retryTimeoutMs >= 0) {
                            if (p_retryTimeoutMs == 0 || System.nanoTime() - startTime >=
                                    p_retryTimeoutMs * 1000 * 1000) {
                                // reset reserved write lock flag
                                p_entry.releaseWriteLock();
                                assert !p_entry.isWriteLockAcquired();

                                // enforce this state in order to get out of here with a consistent lock state
                                while (!p_cidTable.entryAtomicUpdate(p_entry)) {
                                    Thread.yield();
                                    p_cidTable.entryReread(p_entry);

                                    assert !p_entry.isWriteLockAcquired();

                                    p_entry.releaseWriteLock();
                                }

                                // return with current state
                                p_cidTable.entryReread(p_entry);
                                SOP_WRITE_LOCK_TIMEOUTS.inc();
                                return LockStatus.TIMEOUT;
                            }
                        }

                        Thread.yield();
                        retries++;
                        p_cidTable.entryReread(p_entry);
                    }

                    if (retries > 0) {
                        SOP_WRITE_LOCK_RETRIES.add(retries);
                    }

                    // write locked with no readers in section
                    return LockStatus.OK;
                }
            }
            // else: write lock already acquired

            if (p_retryTimeoutMs >= 0) {
                if (p_retryTimeoutMs == 0 || System.nanoTime() - startTime >= p_retryTimeoutMs * 1000 * 1000) {
                    // return with current state
                    p_cidTable.entryReread(p_entry);
                    return LockStatus.TIMEOUT;
                }
            }

            Thread.yield();
            retries++;
            p_cidTable.entryReread(p_entry);
        }
    }

    /**
     * Release an acquired write lock
     *
     * @param p_cidTable
     *         CIDTable instance
     * @param p_entry
     *         Chunk entry of CIDTable to unlock
     */
    private static void releaseWriteLock(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry) {
        p_entry.currentStateInitialState();

        while (true) {
            // invalid state, chunk was deleted but a lock was still acquired
            if (!p_entry.isValid()) {
                throw new IllegalStateException("Release write lock of deleted chunk " + p_entry);
            }

            // invalid lock state
            if (!p_entry.isWriteLockAcquired() || p_entry.areReadLocksAcquired()) {
                throw new IllegalStateException(
                        "Releasing write lock with no write lock acquired or read locks acquired for " + p_entry);
            }

            p_entry.releaseWriteLock();

            // write lock released, try to persist state
            if (p_cidTable.entryAtomicUpdate(p_entry)) {
                break;
            }

            Thread.yield();
            p_cidTable.entryReread(p_entry);
        }
    }

    /**
     * Swap an already acquired write lock for a read lock
     *
     * @param p_cidTable
     *         CIDTable instance
     * @param p_entry
     *         Chunk entry of CIDTable to swap lock on
     * @param p_retryTimeoutMs
     *         -1 = infinite, 0 = one shot, &gt; 0 timeout in ms
     * @return Lock status
     */
    private static LockStatus swapWriteLockForReadLock(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry,
            final int p_retryTimeoutMs) {
        long startTime = 0;

        if (p_retryTimeoutMs > 0) {
            startTime = System.nanoTime();
        }

        SOP_SWAP_WRITE_LOCK_REQS.inc();

        int retries = 0;

        p_entry.currentStateInitialState();

        while (true) {
            // entry turned invalid, e.g. chunk was deleted
            if (!p_entry.isValid()) {
                return LockStatus.INVALID;
            }

            if (!p_entry.isWriteLockAcquired()) {
                throw new IllegalStateException("Swapping write for read lock not possible, no write lock acquired");
            }

            // lock swap
            p_entry.releaseWriteLock();

            if (p_entry.acquireReadLock()) {
                // read lock acquired, try to persist state
                if (p_cidTable.entryAtomicUpdate(p_entry)) {
                    if (retries > 0) {
                        SOP_SWAP_WRITE_LOCK_RETRIES.add(retries);
                    }

                    return LockStatus.OK;
                }
            } else {
                // log once to avoid flooding the log on retries
                if (retries == 0) {
                    LOGGER.warn("Max number of read locks already acquired for %s", p_entry);
                }
            }
            // else: no locks available because too many readers active, we have to wait

            if (p_retryTimeoutMs >= 0) {
                if (p_retryTimeoutMs == 0 || System.nanoTime() - startTime >= p_retryTimeoutMs * 1000 * 1000) {
                    // return with current state
                    p_cidTable.entryReread(p_entry);
                    SOP_SWAP_WRITE_LOCK_TIMEOUTS.inc();
                    return LockStatus.TIMEOUT;
                }
            }

            Thread.yield();
            retries++;

            p_cidTable.entryReread(p_entry);
        }
    }

    /**
     * Swap an already acquired read lock for a write lock
     *
     * @param p_cidTable
     *         CIDTable instance
     * @param p_entry
     *         Chunk entry of CIDTable to swap lock on
     * @param p_retryTimeoutMs
     *         -1 = infinite, 0 = one shot, &gt; 0 timeout in ms
     * @return Lock status
     */
    private static LockStatus swapReadLockForWriteLock(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry,
            final int p_retryTimeoutMs) {
        long startTime = 0;

        if (p_retryTimeoutMs > 0) {
            startTime = System.nanoTime();
        }

        SOP_SWAP_READ_LOCK_REQS.inc();

        int retries = 0;

        p_entry.currentStateInitialState();

        // important: a fully atomic read to write lock swap is not possible and quickly results in deadlocks
        // (lock demotion from write -> read is not an issue here)
        // example: two threads have a read lock acquired and both want to swap it for a write lock. the first
        // thread is successful in doing so and is waiting for the second thread to release his read lock.
        // but, the second thread can't because it also wants to execute the atomic swap and can't acquire the
        // write lock because the first thread already acquired it
        //
        // thus, this lock swap is implemented as an "optimized" version of release read lock, acquire write lock
        // instead of a true atomic lock swap

        // release read lock, first
        while (true) {
            // invalid state, chunk was deleted but a lock was still acquired
            if (!p_entry.isValid()) {
                throw new IllegalStateException("Release read lock of deleted chunk " + p_entry);
            }

            // write lock might be acquired: writer thread blocks all further reader threads and waits for
            // current readers in critical section to exit
            if (!p_entry.areReadLocksAcquired()) {
                throw new IllegalStateException("Releasing read lock with no read locks acquired for " + p_entry);
            }

            p_entry.releaseReadLock();

            // read lock released, try to persist state
            if (p_cidTable.entryAtomicUpdate(p_entry)) {
                break;
            }

            Thread.yield();
            p_cidTable.entryReread(p_entry);
        }

        p_entry.currentStateInitialState();

        // acquire write lock
        while (true) {
            // entry turned invalid, e.g. chunk was deleted
            if (!p_entry.isValid()) {
                return LockStatus.INVALID;
            }

            if (p_entry.acquireWriteLock()) {
                // try persist write lock acquired state
                if (p_cidTable.entryAtomicUpdate(p_entry)) {
                    // wait for all read locks to be released
                    while (p_entry.getReadLockCounter() > 0) {
                        if (p_retryTimeoutMs >= 0) {
                            if (p_retryTimeoutMs == 0 || System.nanoTime() - startTime >=
                                    p_retryTimeoutMs * 1000 * 1000) {
                                // rollback: because we have the write lock acquired, no threads were able to
                                // acquire read locks and we can easily swap the write lock for a read lock to
                                // revert the changes
                                assert p_entry.isWriteLockAcquired();

                                p_entry.releaseWriteLock();

                                if (!p_entry.acquireReadLock()) {
                                    throw new IllegalStateException("No read lock for rollback of lock swap available");
                                }

                                // enforce this state in order to get out of here with a consistent lock state
                                while (!p_cidTable.entryAtomicUpdate(p_entry)) {
                                    Thread.yield();
                                    p_cidTable.entryReread(p_entry);

                                    assert p_entry.isWriteLockAcquired();

                                    p_entry.releaseWriteLock();

                                    if (!p_entry.acquireReadLock()) {
                                        throw new IllegalStateException(
                                                "No read lock for rollback of lock swap available");
                                    }
                                }

                                // return with current state
                                p_cidTable.entryReread(p_entry);
                                SOP_READ_LOCK_TIMEOUTS.inc();
                                return LockStatus.TIMEOUT;
                            }
                        }

                        Thread.yield();
                        retries++;
                        p_cidTable.entryReread(p_entry);
                    }

                    assert !p_entry.areReadLocksAcquired();
                    assert p_entry.isWriteLockAcquired();

                    if (retries > 0) {
                        SOP_READ_LOCK_RETRIES.add(retries);
                    }

                    // write locked with no readers in section
                    return LockStatus.OK;
                }
            }
            // else: write lock already acquired

            if (p_retryTimeoutMs >= 0) {
                if (p_retryTimeoutMs == 0 || System.nanoTime() - startTime >= p_retryTimeoutMs * 1000 * 1000) {
                    // return with current state
                    p_cidTable.entryReread(p_entry);
                    return LockStatus.TIMEOUT;
                }
            }

            Thread.yield();
            retries++;
            p_cidTable.entryReread(p_entry);
        }
    }
}
