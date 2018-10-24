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

import de.hhu.bsinfo.dxmem.data.ChunkID;

/**
 * Cache for translated addresses (CID to address)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public final class CIDTranslationCache {
    private Cache[] m_cache;

    /**
     * Constructor
     */
    public CIDTranslationCache() {
        // NOTE: 10 seems to be a good value because it doesn't add too much overhead when creating huge ranges of chunks
        // but still allows 10 * 4096 translations to be cached for fast lookup and gets/puts
        // (value determined by profiling the application)
        m_cache = new Cache[1024];

        for (int i = 0; i < m_cache.length; i++) {
            m_cache[i] = new Cache(10);
        }
    }

    /**
     * Try to get the table level 0 entry for the chunk id
     *
     * @param p_chunkID
     *         Chunk id for cache lookup of table level 0
     * @return Address of level 0 table or -1 if not cached
     */
    long getTableLevel0(final long p_chunkID) {
        return 0;
        //        return m_cache[(int) Thread.currentThread().getId()].getTableLevel0(p_chunkID);
    }

    /**
     * Put a new entry into the cache
     *
     * @param p_chunkID
     *         Chunk id of the table level 0 to be cached
     * @param p_addressTable
     *         Address of the level 0 table
     */
    void putTableLevel0(final long p_chunkID, final long p_addressTable) {
        //        m_cache[(int) Thread.currentThread().getId()].putTableLevel0(p_chunkID, p_addressTable);
    }

    /**
     * Thread local cache
     */
    private static final class Cache {
        private long[] m_chunkIDs;
        private long[] m_tableLevel0Addr;
        private int m_cachePos;

        /**
         * Constructor
         *
         * @param p_size
         *         Number of entries for the cache
         */
        Cache(final int p_size) {
            m_chunkIDs = new long[p_size];
            m_tableLevel0Addr = new long[p_size];
            m_cachePos = 0;

            for (int i = 0; i < p_size; i++) {
                m_chunkIDs[i] = ChunkID.INVALID_ID;
                m_tableLevel0Addr[i] = Address.INVALID;
            }
        }

        /**
         * Try to get the table level 0 entry for the chunk id
         *
         * @param p_chunkID
         *         Chunk id for cache lookup of table level 0
         * @return Address of level 0 table or -1 if not cached
         */
        long getTableLevel0(final long p_chunkID) {
            long tableLevel0IDRange = p_chunkID >> CIDTable.BITS_PER_LID_LEVEL;

            for (int i = 0; i < m_chunkIDs.length; i++) {
                if (m_chunkIDs[i] == tableLevel0IDRange) {
                    return m_tableLevel0Addr[i];
                }
            }

            return Address.INVALID;
        }

        /**
         * Put a new entry into the cache
         *
         * @param p_chunkID
         *         Chunk id of the table level 0 to be cached
         * @param p_addressTable
         *         Address of the level 0 table
         */
        void putTableLevel0(final long p_chunkID, final long p_addressTable) {
            m_chunkIDs[m_cachePos] = p_chunkID >> CIDTable.BITS_PER_LID_LEVEL;
            m_tableLevel0Addr[m_cachePos] = p_addressTable;
            m_cachePos = (m_cachePos + 1) % m_chunkIDs.length;
        }
    }
}
