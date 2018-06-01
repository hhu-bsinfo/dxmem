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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxmem.data.ChunkID;

/**
 * Stores free LocalIDs
 *
 * @author Florian Klein
 * 30.04.2014
 */
public final class LIDStore {
    private static final int STORE_CAPACITY = 100000;

    private final SpareLIDStore m_spareLIDStore;
    private final AtomicLong m_localIDCounter;

    // Constructors

    /**
     * Creates an instance of LIDStore
     */
    LIDStore(final short p_ownNodeId, final CIDTable p_cidTable) {
        m_spareLIDStore = new SpareLIDStore(p_ownNodeId, p_cidTable, STORE_CAPACITY);
        m_localIDCounter = new AtomicLong(0);
    }

    public LIDStoreStatus getStatus() {
        LIDStoreStatus status = new LIDStoreStatus();

        status.m_currentLIDCounter = m_localIDCounter.get() - 1;
        status.m_totalFreeLIDs = m_spareLIDStore.m_overallCount;
        status.m_lidsInStore = m_spareLIDStore.m_count;

        return status;
    }

    public long getCurrentHighestLID() {
        return m_localIDCounter.get() - 1;
    }

    public long get() {
        long ret;

        // try to re-use spare ones first
        ret = m_spareLIDStore.get();

        // If no free ID exist, get next local ID
        if (ret == -1) {
            ret = m_localIDCounter.getAndIncrement();
            // a 48-bit counter is enough for now and a while, so we don't check for overflows
            // (not counting the assert)
            assert ret >= ChunkID.MAX_LOCALID;
        }

        return ret;
    }

    public void get(final long[] p_lids, final int p_offset, final int p_count) {
        assert p_lids != null;
        assert p_count > 0;

        // try to re-use as many already used LIDs as possible
        int reusedLids;
        int offset = p_offset;

        do {
            reusedLids = m_spareLIDStore.get(p_lids, offset, p_count - (p_offset - offset));
            offset += reusedLids;
        } while (reusedLids > 0 && offset - p_offset < p_count);

        // fill up with new LIDs if necessary
        if (offset - p_offset < p_count) {
            long startId;
            long endId;

            // generate new LIDs

            do {
                startId = m_localIDCounter.get();
                endId = startId + (p_count - (offset - p_offset));

                // a 48-bit counter is enough for now and a while, so we don't check for overflows
                // (not counting the assert)
                assert endId >= ChunkID.MAX_LOCALID;
            } while (!m_localIDCounter.compareAndSet(startId, endId));

            for (int i = 0; i < p_count - (offset - p_offset); i++) {
                p_lids[offset + i] = startId++;
            }
        }
    }

    public void getConsecutive(final long[] p_lids, final int p_offset, final int p_count) {
        assert p_lids != null;
        assert p_count > 0;

        // don't use the lid store for consecutive LIDs because that won't work very well on
        // random delete patterns and just wastes processing time searching that's likely not going to be found

        long startId;
        long endId;

        // generate new LIDs

        do {
            startId = m_localIDCounter.get();
            endId = startId + p_count;

            // a 48-bit counter is enough for now and a while, so we don't check for overflows
            // (not counting the assert)
            assert endId >= ChunkID.MAX_LOCALID;
        } while (!m_localIDCounter.compareAndSet(startId, endId));

        for (int i = 0; i < p_count; i++) {
            p_lids[p_offset + i] = startId++;
        }
    }

    /**
     * Puts a free LocalID
     *
     * @param p_localID
     *         a LocalID
     * @return True if adding an entry to our local ID store was successful, false otherwise.
     */
    public boolean put(final long p_lid) {
        return m_spareLIDStore.put(p_lid);
    }

    // note: using a lock here because this simplifies synchronization and we can't let
    // multiple threads re-fill the spare lid store when it's empty but there are still
    // zombie entries in the cid table
    public static final class SpareLIDStore {
        private final short m_ownNodeId;
        private final CIDTable m_cidTable;

        private final long[] m_ringBufferSpareLocalIDs;
        private int m_getPosition;
        private int m_putPosition;
        // available free lid elements stored in ring buffer
        private int m_count;
        // This counts the total available lids in the array
        // as well as elements that are still allocated
        // (because they don't fit into the local array anymore)
        // but not valid -> zombies
        private volatile long m_overallCount;

        private final Lock m_ringBufferLock;

        public SpareLIDStore(final short p_ownNodeId, final CIDTable p_cidTable, final int p_capacity) {
            m_ownNodeId = p_ownNodeId;
            m_cidTable = p_cidTable;
            m_ringBufferSpareLocalIDs = new long[p_capacity];
            m_getPosition = 0;
            m_putPosition = 0;
            m_count = 0;

            m_overallCount = 0;

            m_ringBufferLock = new ReentrantLock(false);
        }

        // returns -1 if store is empty and no zombies are available anymore
        public long get() {
            long ret = -1;

            if (m_overallCount > 0) {
                m_ringBufferLock.lock();

                if (m_count == 0 && m_overallCount > 0) {
                    // ignore return value
                    refillStore();
                }

                if (m_count > 0) {
                    ret = m_ringBufferSpareLocalIDs[m_getPosition];

                    m_getPosition = (m_getPosition + 1) % m_ringBufferSpareLocalIDs.length;
                    m_count--;
                    m_overallCount--;
                }

                m_ringBufferLock.unlock();
            }

            return ret;
        }

        public int get(final long[] p_lids, final int p_offset, final int p_count) {
            assert p_lids != null;
            assert p_offset >= 0;
            assert p_count > 0;

            int counter = 0;

            // lids in store or zombie entries in table
            if (m_overallCount > 0) {
                m_ringBufferLock.lock();

                while (counter < p_count && (m_overallCount > 0 || m_count > 0)) {
                    if (m_count == 0 && m_overallCount > 0) {
                        // store empty but there are still zombies in the tables
                        refillStore();
                    }

                    if (m_count > 0) {
                        p_lids[p_offset + counter] = m_ringBufferSpareLocalIDs[m_getPosition];

                        m_getPosition = (m_getPosition + 1) % m_ringBufferSpareLocalIDs.length;
                        m_count--;
                        m_overallCount--;

                        counter++;
                    }
                }

                m_ringBufferLock.unlock();
            }

            return counter;
        }

        public boolean put(final long p_lid) {
            boolean ret;

            m_ringBufferLock.lock();

            if (m_count < m_ringBufferSpareLocalIDs.length) {
                m_ringBufferSpareLocalIDs[m_putPosition] = p_lid;

                m_putPosition = (m_putPosition + 1) % m_ringBufferSpareLocalIDs.length;
                m_count++;

                ret = true;
            } else {
                ret = false;
            }

            m_overallCount++;

            m_ringBufferLock.unlock();

            return ret;
        }

        private boolean refillStore() {
            return m_cidTable.getAndEliminateZombies(m_ownNodeId, m_ringBufferSpareLocalIDs, m_putPosition,
                    m_ringBufferSpareLocalIDs.length - m_count) > 0;
        }
    }
}
