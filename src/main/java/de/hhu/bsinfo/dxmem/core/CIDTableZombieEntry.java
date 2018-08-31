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
 * Helper class for handling zombie entries (table level 0) in the CIDTable. This class caches the entry read from
 * memory, only. Any changes applied are NOT automatically written back to memory.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class CIDTableZombieEntry {
    static final long RAW_VALUE = -1;

    private long m_pointer;
    private long m_cid;

    /**
     * Constructor
     */
    CIDTableZombieEntry() {
        clear();
    }

    /**
     * Constructor
     *
     * @param p_pointer
     *         Pointer (address) to zombie entry
     * @param p_cid
     *         Cid of entry
     */
    CIDTableZombieEntry(final long p_pointer, final long p_cid) {
        m_pointer = p_pointer;
        m_cid = p_cid;
    }

    /**
     * Clear the object
     */
    public void clear() {
        m_pointer = Address.INVALID;
        m_cid = ChunkID.INVALID_ID;
    }

    /**
     * Set the object.
     *
     * @param p_pointer
     *         Pointer (address) to entry
     * @param p_cid
     *         Cid of zombie
     */
    public void set(final long p_pointer, final long p_cid) {
        m_pointer = p_pointer;
        m_cid = p_cid;
    }

    /**
     * Get the pointer
     *
     * @return Pointer
     */
    public long getPointer() {
        return m_pointer;
    }

    /**
     * Get the cid
     *
     * @return Cid
     */
    public long getCID() {
        return m_cid;
    }

    @Override
    public String toString() {
        return "m_pointer " + Address.toHexString(m_pointer) + ", m_cid " + ChunkID.toHexString(m_cid);
    }
}
