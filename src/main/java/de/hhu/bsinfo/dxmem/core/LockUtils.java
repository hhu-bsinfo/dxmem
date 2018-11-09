/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxmem.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Utility class for locking chunks in CIDTable
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public final class LockUtils {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LockUtils.class.getSimpleName());

    private static final ValuePool SOP_READ_LOCK_REQS = new ValuePool(DXMem.class, "ReadLockReqs");
    private static final ValuePool SOP_READ_LOCK_RETRIES = new ValuePool(DXMem.class, "ReadLockRetries");
    private static final ValuePool SOP_READ_LOCK_TIMEOUTS = new ValuePool(DXMem.class, "ReadLockTimeouts");
    private static final ValuePool SOP_WRITE_LOCK_REQS = new ValuePool(DXMem.class, "WriteLockReqs");
    private static final ValuePool SOP_WRITE_LOCK_RETRIES = new ValuePool(DXMem.class, "WriteLockRetries");
    private static final ValuePool SOP_WRITE_LOCK_TIMEOUTS = new ValuePool(DXMem.class, "WriteLockTimeouts");

    static {
        StatisticsManager.get().registerOperation(DXMem.class, SOP_READ_LOCK_REQS);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_READ_LOCK_RETRIES);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_READ_LOCK_TIMEOUTS);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_WRITE_LOCK_REQS);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_WRITE_LOCK_RETRIES);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_WRITE_LOCK_TIMEOUTS);
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
     * Private constructor for utility class
     */
    private LockUtils() {

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
    public static LockStatus acquireReadLock(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry,
            final int p_retryTimeoutMs) {
        long startTime = 0;

        if (p_retryTimeoutMs > 0) {
            startTime = System.nanoTime();
        }

        SOP_READ_LOCK_REQS.inc();

        int retries = 0;

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
    public static void releaseReadLock(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry) {
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
    public static LockStatus acquireWriteLock(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry,
            final int p_retryTimeoutMs) {
        long startTime = 0;

        if (p_retryTimeoutMs > 0) {
            startTime = System.nanoTime();
        }

        SOP_WRITE_LOCK_REQS.inc();

        int retries = 0;

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
    public static void releaseWriteLock(final CIDTable p_cidTable, final CIDTableChunkEntry p_entry) {
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
}
