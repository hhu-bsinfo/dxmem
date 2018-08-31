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

/**
 * Thread local pool for chunk entry objects (to avoid allocations)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class CIDTableEntryPool {
    private static final int MAX_THREAD_COUNT = 1024;

    private CIDTableChunkEntry[] m_pool;

    /**
     * Constructor
     */
    CIDTableEntryPool() {
        m_pool = new CIDTableChunkEntry[MAX_THREAD_COUNT];

        for (int i = 0; i < m_pool.length; i++) {
            m_pool[i] = new CIDTableChunkEntry();
        }
    }

    /**
     * Get an entry from the pool
     *
     * @return Chunk entry object
     */
    public CIDTableChunkEntry get() {
        try {
            CIDTableChunkEntry entry = m_pool[(int) Thread.currentThread().getId()];
            entry.clear();
            return entry;
        } catch (ArrayIndexOutOfBoundsException ignored) {
            throw new MemoryRuntimeException("Thread IDs (and probably thread count) exceeding max pool size " +
                    MAX_THREAD_COUNT);
        }
    }
}
