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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;

public class LockTest {
    @Test(timeout = 2000)
    public void readLockSimpleST() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(1024 * 1024);
        CIDTable cidTable = new CIDTable((short) 0, heap);

        CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();
        long cid = 0;

        Assert.assertTrue(heap.malloc(1, chunkEntry));

        Assert.assertTrue(cidTable.insert(cid, chunkEntry));
        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());

        LockManager.LockStatus status = LockManager.executeBeforeOp(cidTable, chunkEntry,
                ChunkLockOperation.READ_LOCK_ACQ_PRE_OP, -1);

        Assert.assertEquals(LockManager.LockStatus.OK, status);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertTrue(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(1, chunkEntry.getReadLockCounter());

        LockManager.executeAfterOp(cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_REL_POST_OP, -1);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());
    }

    @Test(timeout = 20000)
    public void readLockLoopST() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(1024 * 1024);
        CIDTable cidTable = new CIDTable((short) 0, heap);

        CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();
        long cid = 0;

        Assert.assertTrue(heap.malloc(1, chunkEntry));

        Assert.assertTrue(cidTable.insert(cid, chunkEntry));
        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());

        for (int i = 0; i < 1000; i++) {
            LockManager.LockStatus status = LockManager.executeBeforeOp(cidTable, chunkEntry,
                    ChunkLockOperation.READ_LOCK_ACQ_PRE_OP, -1);

            Assert.assertEquals(LockManager.LockStatus.OK, status);

            cidTable.entryReread(chunkEntry);

            Assert.assertTrue(chunkEntry.isValid());
            Assert.assertFalse(chunkEntry.isWriteLockAcquired());
            Assert.assertTrue(chunkEntry.areReadLocksAcquired());
            Assert.assertEquals(1, chunkEntry.getReadLockCounter());

            LockManager.executeAfterOp(cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_REL_POST_OP, -1);

            cidTable.entryReread(chunkEntry);

            Assert.assertTrue(chunkEntry.isValid());
            Assert.assertFalse(chunkEntry.isWriteLockAcquired());
            Assert.assertFalse(chunkEntry.areReadLocksAcquired());
            Assert.assertEquals(0, chunkEntry.getReadLockCounter());
        }
    }

    @Test(timeout = 2000)
    public void writeLockSimpleST() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(1024 * 1024);
        CIDTable cidTable = new CIDTable((short) 0, heap);

        CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();
        long cid = 0;

        Assert.assertTrue(heap.malloc(1, chunkEntry));

        Assert.assertTrue(cidTable.insert(cid, chunkEntry));
        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());

        LockManager.LockStatus status = LockManager.executeBeforeOp(cidTable, chunkEntry,
                ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, -1);

        Assert.assertEquals(LockManager.LockStatus.OK, status);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertTrue(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());

        LockManager.executeAfterOp(cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());
    }

    @Test(timeout = 20000)
    public void writeLockLoopST() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(1024 * 1024);
        CIDTable cidTable = new CIDTable((short) 0, heap);

        CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();
        long cid = 0;

        Assert.assertTrue(heap.malloc(1, chunkEntry));

        Assert.assertTrue(cidTable.insert(cid, chunkEntry));
        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());

        for (int i = 0; i < 1000; i++) {
            LockManager.LockStatus status = LockManager.executeBeforeOp(cidTable, chunkEntry,
                    ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, -1);

            Assert.assertEquals(LockManager.LockStatus.OK, status);

            cidTable.entryReread(chunkEntry);

            Assert.assertTrue(chunkEntry.isValid());
            Assert.assertTrue(chunkEntry.isWriteLockAcquired());
            Assert.assertFalse(chunkEntry.areReadLocksAcquired());
            Assert.assertEquals(0, chunkEntry.getReadLockCounter());

            LockManager.executeAfterOp(cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1);

            cidTable.entryReread(chunkEntry);

            Assert.assertTrue(chunkEntry.isValid());
            Assert.assertFalse(chunkEntry.isWriteLockAcquired());
            Assert.assertFalse(chunkEntry.areReadLocksAcquired());
            Assert.assertEquals(0, chunkEntry.getReadLockCounter());
        }
    }

    @Test(timeout = 2000)
    public void readLockSwapSimpleST() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(1024 * 1024);
        CIDTable cidTable = new CIDTable((short) 0, heap);

        CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();
        long cid = 0;

        Assert.assertTrue(heap.malloc(1, chunkEntry));

        Assert.assertTrue(cidTable.insert(cid, chunkEntry));
        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());

        LockManager.LockStatus status = LockManager.executeBeforeOp(cidTable, chunkEntry,
                ChunkLockOperation.READ_LOCK_ACQ_PRE_OP, -1);

        Assert.assertEquals(LockManager.LockStatus.OK, status);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertTrue(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(1, chunkEntry.getReadLockCounter());

        status = LockManager.executeBeforeOp(cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_SWAP_PRE_OP, -1);

        Assert.assertEquals(LockManager.LockStatus.OK, status);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertTrue(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());

        status = LockManager.executeAfterOp(cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1);

        Assert.assertEquals(LockManager.LockStatus.OK, status);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());
    }

    @Test(timeout = 20000)
    public void readLockSwapLoopST() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(1024 * 1024);
        CIDTable cidTable = new CIDTable((short) 0, heap);

        CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();
        long cid = 0;

        Assert.assertTrue(heap.malloc(1, chunkEntry));

        Assert.assertTrue(cidTable.insert(cid, chunkEntry));
        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());

        for (int i = 0; i < 1000; i++) {
            LockManager.LockStatus status = LockManager.executeBeforeOp(cidTable, chunkEntry,
                    ChunkLockOperation.READ_LOCK_ACQ_PRE_OP, -1);

            Assert.assertEquals(LockManager.LockStatus.OK, status);

            cidTable.entryReread(chunkEntry);

            Assert.assertTrue(chunkEntry.isValid());
            Assert.assertFalse(chunkEntry.isWriteLockAcquired());
            Assert.assertTrue(chunkEntry.areReadLocksAcquired());
            Assert.assertEquals(1, chunkEntry.getReadLockCounter());

            status = LockManager.executeBeforeOp(cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_SWAP_PRE_OP, -1);

            Assert.assertEquals(LockManager.LockStatus.OK, status);

            cidTable.entryReread(chunkEntry);

            Assert.assertTrue(chunkEntry.isValid());
            Assert.assertTrue(chunkEntry.isWriteLockAcquired());
            Assert.assertFalse(chunkEntry.areReadLocksAcquired());
            Assert.assertEquals(0, chunkEntry.getReadLockCounter());

            status = LockManager.executeAfterOp(cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1);

            Assert.assertEquals(LockManager.LockStatus.OK, status);

            cidTable.entryReread(chunkEntry);

            Assert.assertTrue(chunkEntry.isValid());
            Assert.assertFalse(chunkEntry.isWriteLockAcquired());
            Assert.assertFalse(chunkEntry.areReadLocksAcquired());
            Assert.assertEquals(0, chunkEntry.getReadLockCounter());
        }
    }

    @Test(timeout = 2000)
    public void writeLockSwapSimpleST() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(1024 * 1024);
        CIDTable cidTable = new CIDTable((short) 0, heap);

        CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();
        long cid = 0;

        Assert.assertTrue(heap.malloc(1, chunkEntry));

        Assert.assertTrue(cidTable.insert(cid, chunkEntry));
        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());

        LockManager.LockStatus status = LockManager.executeBeforeOp(cidTable, chunkEntry,
                ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, -1);

        Assert.assertEquals(LockManager.LockStatus.OK, status);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertTrue(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());

        status = LockManager.executeBeforeOp(cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_SWAP_PRE_OP, -1);

        Assert.assertEquals(LockManager.LockStatus.OK, status);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertTrue(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(1, chunkEntry.getReadLockCounter());

        status = LockManager.executeAfterOp(cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_REL_POST_OP, -1);

        Assert.assertEquals(LockManager.LockStatus.OK, status);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());
    }

    @Test(timeout = 20000)
    public void writeLockSwapLoopST() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(1024 * 1024);
        CIDTable cidTable = new CIDTable((short) 0, heap);

        CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();
        long cid = 0;

        Assert.assertTrue(heap.malloc(1, chunkEntry));

        Assert.assertTrue(cidTable.insert(cid, chunkEntry));
        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());

        for (int i = 0; i < 1000; i++) {
            LockManager.LockStatus status = LockManager.executeBeforeOp(cidTable, chunkEntry,
                    ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, -1);

            Assert.assertEquals(LockManager.LockStatus.OK, status);

            cidTable.entryReread(chunkEntry);

            Assert.assertTrue(chunkEntry.isValid());
            Assert.assertTrue(chunkEntry.isWriteLockAcquired());
            Assert.assertFalse(chunkEntry.areReadLocksAcquired());
            Assert.assertEquals(0, chunkEntry.getReadLockCounter());

            status = LockManager.executeBeforeOp(cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_SWAP_PRE_OP, -1);

            Assert.assertEquals(LockManager.LockStatus.OK, status);

            cidTable.entryReread(chunkEntry);

            Assert.assertTrue(chunkEntry.isValid());
            Assert.assertFalse(chunkEntry.isWriteLockAcquired());
            Assert.assertTrue(chunkEntry.areReadLocksAcquired());
            Assert.assertEquals(1, chunkEntry.getReadLockCounter());

            status = LockManager.executeAfterOp(cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_REL_POST_OP, -1);

            Assert.assertEquals(LockManager.LockStatus.OK, status);

            cidTable.entryReread(chunkEntry);

            Assert.assertTrue(chunkEntry.isValid());
            Assert.assertFalse(chunkEntry.isWriteLockAcquired());
            Assert.assertFalse(chunkEntry.areReadLocksAcquired());
            Assert.assertEquals(0, chunkEntry.getReadLockCounter());
        }
    }

    @Test(timeout = 20000)
    public void readLockMT2() {
        run(2, 0, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void readLockMT4() {
        run(4, 0, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void writeLockMT2() {
        run(0, 2, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void writeLockMT4() {
        run(0, 4, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void readWriteLockMT1_1() {
        run(1, 1, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void readWriteLockMT3_1() {
        run(3, 1, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void readWriteLockMT2_2() {
        run(2, 2, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void readWriteLockMT3_3() {
        run(3, 3, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void readWriteLockMT4_2() {
        run(4, 2, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void readWriteLockMT1_5() {
        run(1, 5, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void readWriteLockMT5_1() {
        run(5, 1, 0, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void swapReadLockMT2() {
        run(0, 0, 2, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void swapReadLockMT4() {
        run(0, 0, 4, 0, 10000000);
    }

    @Test(timeout = 20000)
    public void swapWriteLockMT2() {
        run(0, 0, 0, 2, 10000000);
    }

    @Test(timeout = 20000)
    public void swapWriteLockMT4() {
        run(0, 0, 0, 4, 10000000);
    }

    @Test(timeout = 20000)
    public void swapReadWriteLockMT1_1() {
        run(0, 0, 1, 1, 100);
    }

    @Test(timeout = 20000)
    public void swapReadWriteLockMT2_2() {
        run(0, 0, 1, 1, 10000000);
    }

    @Test(timeout = 20000)
    public void mixedLockMT() {
        run(1, 1, 1, 1, 10000000);
    }

    @Test(timeout = 10000)
    public void readWriteTryLock() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(1024 * 1024);
        CIDTable cidTable = new CIDTable((short) 0, heap);

        CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();
        long cid = 0;

        Assert.assertTrue(heap.malloc(1, chunkEntry));

        Assert.assertTrue(cidTable.insert(cid, chunkEntry));
        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());

        LockManager.LockStatus status = LockManager.executeBeforeOp(cidTable, chunkEntry,
                ChunkLockOperation.READ_LOCK_ACQ_PRE_OP, -1);

        Assert.assertEquals(LockManager.LockStatus.OK, status);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertTrue(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(1, chunkEntry.getReadLockCounter());

        // try to get write lock
        long time = System.nanoTime();

        status = LockManager.executeBeforeOp(cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, 1000);

        double totalTime = (System.nanoTime() - time) / 1000.0 / 1000.0;
        System.out.printf("Timeout write lock: %f ms\n", totalTime);
        Assert.assertEquals(1000, totalTime, 1.0);

        cidTable.entryReread(chunkEntry);

        Assert.assertEquals(LockManager.LockStatus.TIMEOUT, status);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertTrue(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(1, chunkEntry.getReadLockCounter());

        LockManager.executeAfterOp(cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_REL_POST_OP, -1);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());

        // vice versa: get write lock and try read lock
        status = LockManager.executeBeforeOp(cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, -1);

        Assert.assertEquals(LockManager.LockStatus.OK, status);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertTrue(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());

        // try read lock
        time = System.nanoTime();

        status = LockManager.executeBeforeOp(cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_ACQ_PRE_OP, 500);

        totalTime = (System.nanoTime() - time) / 1000.0 / 1000.0;
        System.out.printf("Timeout read lock: %f ms\n", totalTime);
        Assert.assertEquals(500, totalTime, 1.0);

        Assert.assertEquals(LockManager.LockStatus.TIMEOUT, status);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertTrue(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());

        LockManager.executeAfterOp(cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1);

        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());
        Assert.assertFalse(chunkEntry.isWriteLockAcquired());
        Assert.assertFalse(chunkEntry.areReadLocksAcquired());
        Assert.assertEquals(0, chunkEntry.getReadLockCounter());
    }

    private void run(final int p_numReadThreads, final int p_numWriteThreads, final int p_numSwapReadLockThreads,
            final int p_numSwapWriteLockThreads, final int p_iterations) {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(1024 * 1024);
        CIDTable cidTable = new CIDTable((short) 0, heap);

        CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();
        long cid = 0;

        Assert.assertTrue(heap.malloc(1, chunkEntry));

        Assert.assertTrue(cidTable.insert(cid, chunkEntry));
        cidTable.entryReread(chunkEntry);

        Assert.assertTrue(chunkEntry.isValid());

        Thread[] threads = new Thread[p_numReadThreads + p_numWriteThreads + p_numSwapReadLockThreads +
                p_numSwapWriteLockThreads];

        for (int i = 0; i < p_numReadThreads; i++) {
            threads[i] = new ReadLockThread(i, p_iterations,
                    p_numReadThreads + p_numSwapReadLockThreads + p_numSwapWriteLockThreads, cidTable);
        }

        for (int i = 0; i < p_numWriteThreads; i++) {
            threads[p_numReadThreads + i] = new WriteLockThread(i, p_iterations,
                    p_numReadThreads + p_numSwapReadLockThreads + p_numSwapWriteLockThreads, cidTable);
        }

        for (int i = 0; i < p_numSwapReadLockThreads; i++) {
            threads[p_numReadThreads + p_numWriteThreads + i] = new SwapReadLockThread(i, p_iterations,
                    p_numReadThreads + p_numSwapReadLockThreads + p_numSwapWriteLockThreads, cidTable);
        }

        for (int i = 0; i < p_numSwapWriteLockThreads; i++) {
            threads[p_numReadThreads + p_numWriteThreads + p_numSwapReadLockThreads + i] = new SwapWriteLockThread(i,
                    p_iterations, p_numReadThreads + p_numSwapReadLockThreads + p_numSwapWriteLockThreads, cidTable);
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
            }
        }
    }

    private static class ReadLockThread extends Thread {
        private final int m_iterations;
        private final int m_maxReaders;
        private final CIDTable m_cidTable;

        ReadLockThread(final int p_id, final int p_iterations, final int p_maxReaders,
                final CIDTable p_cidTable) {
            super("ReadLock-" + p_id);

            m_iterations = p_iterations;
            m_maxReaders = p_maxReaders;
            m_cidTable = p_cidTable;
        }

        @Override
        public void run() {
            CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();

            m_cidTable.translate(0, chunkEntry);

            for (int i = 0; i < m_iterations; i++) {
                LockManager.LockStatus status = LockManager.executeBeforeOp(m_cidTable, chunkEntry,
                        ChunkLockOperation.READ_LOCK_ACQ_PRE_OP, -1);

                Assert.assertEquals(LockManager.LockStatus.OK, status);

                Assert.assertTrue(chunkEntry.isValid());
                Assert.assertFalse(chunkEntry.isWriteLockAcquired());
                Assert.assertTrue(chunkEntry.areReadLocksAcquired());
                Assert.assertTrue(chunkEntry.getReadLockCounter() > 0);
                Assert.assertTrue(chunkEntry.getReadLockCounter() <= m_maxReaders);

                status = LockManager.executeAfterOp(m_cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_REL_POST_OP,
                        -1);

                Assert.assertEquals(LockManager.LockStatus.OK, status);

                Assert.assertTrue(chunkEntry.isValid());
                // write lock might be acquired: writer thread blocks all further reader threads and waits for
                // current readers in critical section to exit
                Assert.assertTrue(chunkEntry.getReadLockCounter() >= 0);
                Assert.assertTrue(chunkEntry.getReadLockCounter() <= m_maxReaders);
            }
        }
    }

    private static class WriteLockThread extends Thread {
        private final int m_iterations;
        private final int m_maxReaders;
        private final CIDTable m_cidTable;

        WriteLockThread(final int p_id, final int p_iterations, final int p_maxReaders,
                final CIDTable p_cidTable) {
            super("WriteLock-" + p_id);

            m_iterations = p_iterations;
            m_maxReaders = p_maxReaders;
            m_cidTable = p_cidTable;
        }

        @Override
        public void run() {
            CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();

            m_cidTable.translate(0, chunkEntry);

            for (int i = 0; i < m_iterations; i++) {
                LockManager.LockStatus status = LockManager.executeBeforeOp(m_cidTable, chunkEntry,
                        ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, -1);

                Assert.assertEquals(LockManager.LockStatus.OK, status);

                Assert.assertTrue(chunkEntry.isValid());
                Assert.assertTrue(chunkEntry.isWriteLockAcquired());
                Assert.assertFalse(chunkEntry.areReadLocksAcquired());
                Assert.assertEquals(0, chunkEntry.getReadLockCounter());

                status = LockManager.executeAfterOp(m_cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_REL_POST_OP,
                        -1);

                Assert.assertEquals(LockManager.LockStatus.OK, status);

                Assert.assertTrue(chunkEntry.isValid());
                Assert.assertFalse(chunkEntry.isWriteLockAcquired());
                Assert.assertTrue(chunkEntry.getReadLockCounter() >= 0);
                Assert.assertTrue(chunkEntry.getReadLockCounter() <= m_maxReaders);
            }
        }
    }

    private static class SwapWriteLockThread extends Thread {
        private final int m_iterations;
        private final int m_maxReaders;
        private final CIDTable m_cidTable;

        SwapWriteLockThread(final int p_id, final int p_iterations, final int p_maxReaders,
                final CIDTable p_cidTable) {
            super("SwapWriteLock-" + p_id);

            m_iterations = p_iterations;
            m_maxReaders = p_maxReaders;
            m_cidTable = p_cidTable;
        }

        @Override
        public void run() {
            CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();

            m_cidTable.translate(0, chunkEntry);

            for (int i = 0; i < m_iterations; i++) {
                LockManager.LockStatus status = LockManager.executeBeforeOp(m_cidTable, chunkEntry,
                        ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, -1);

                Assert.assertEquals(LockManager.LockStatus.OK, status);

                Assert.assertTrue(chunkEntry.isValid());
                Assert.assertTrue(chunkEntry.isWriteLockAcquired());
                Assert.assertFalse(chunkEntry.areReadLocksAcquired());
                Assert.assertEquals(0, chunkEntry.getReadLockCounter());

                status = LockManager.executeAfterOp(m_cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_SWAP_POST_OP,
                        -1);

                Assert.assertEquals(LockManager.LockStatus.OK, status);

                Assert.assertTrue(chunkEntry.isValid());
                Assert.assertFalse(chunkEntry.isWriteLockAcquired());
                Assert.assertTrue(chunkEntry.getReadLockCounter() >= 0);
                Assert.assertTrue(chunkEntry.getReadLockCounter() <= m_maxReaders);

                status = LockManager.executeAfterOp(m_cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_REL_POST_OP,
                        -1);

                Assert.assertEquals(LockManager.LockStatus.OK, status);

                Assert.assertTrue(chunkEntry.isValid());
                // When used in combination with another thread here, one could already have a write lock acquired
                // Assert.assertFalse(chunkEntry.isWriteLockAcquired());
                Assert.assertTrue(chunkEntry.getReadLockCounter() >= 0);
                Assert.assertTrue(chunkEntry.getReadLockCounter() <= m_maxReaders);
            }
        }
    }

    private static class SwapReadLockThread extends Thread {
        private final int m_iterations;
        private final int m_maxReaders;
        private final CIDTable m_cidTable;

        SwapReadLockThread(final int p_id, final int p_iterations, final int p_maxReaders,
                final CIDTable p_cidTable) {
            super("SwapReadLock-" + p_id);

            m_iterations = p_iterations;
            m_maxReaders = p_maxReaders;
            m_cidTable = p_cidTable;
        }

        @Override
        public void run() {
            CIDTableChunkEntry chunkEntry = new CIDTableChunkEntry();

            m_cidTable.translate(0, chunkEntry);

            for (int i = 0; i < m_iterations; i++) {
                LockManager.LockStatus status = LockManager.executeBeforeOp(m_cidTable, chunkEntry,
                        ChunkLockOperation.READ_LOCK_ACQ_PRE_OP, -1);

                Assert.assertEquals(LockManager.LockStatus.OK, status);

                Assert.assertTrue(chunkEntry.isValid());
                Assert.assertFalse(chunkEntry.isWriteLockAcquired());
                Assert.assertTrue(chunkEntry.areReadLocksAcquired());
                Assert.assertTrue(chunkEntry.getReadLockCounter() > 0);
                Assert.assertTrue(chunkEntry.getReadLockCounter() <= m_maxReaders);

                status = LockManager.executeBeforeOp(m_cidTable, chunkEntry, ChunkLockOperation.READ_LOCK_SWAP_PRE_OP,
                        -1);

                Assert.assertEquals(LockManager.LockStatus.OK, status);

                Assert.assertTrue(chunkEntry.isValid());
                Assert.assertTrue(chunkEntry.isWriteLockAcquired());
                Assert.assertFalse(chunkEntry.areReadLocksAcquired());
                Assert.assertEquals(0, chunkEntry.getReadLockCounter());

                status = LockManager.executeAfterOp(m_cidTable, chunkEntry, ChunkLockOperation.WRITE_LOCK_REL_POST_OP,
                        -1);

                Assert.assertEquals(LockManager.LockStatus.OK, status);

                Assert.assertTrue(chunkEntry.isValid());
                Assert.assertFalse(chunkEntry.isWriteLockAcquired());
                Assert.assertFalse(chunkEntry.areReadLocksAcquired());
                Assert.assertEquals(0, chunkEntry.getReadLockCounter());
            }
        }
    }
}
