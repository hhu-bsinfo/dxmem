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
 * Data class defining a heap area enclosing data/chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class HeapArea {
    private long m_startAddress;
    private long m_endAddress;

    /**
     * Constructor
     *
     * @param p_startAddress
     *         Start address of heap area (including)
     * @param p_endAddress
     *         End address of heap area (excluding)
     */
    HeapArea(final long p_startAddress, final long p_endAddress) {
        m_startAddress = p_startAddress;
        m_endAddress = p_endAddress;
    }

    /**
     * Get the start address (including)
     *
     * @return Start address
     */
    public long getStartAddress() {
        return m_startAddress;
    }

    /**
     * Get the end address (excluding)
     *
     * @return End address
     */
    public long getEndAddress() {
        return m_endAddress;
    }

    @Override
    public String toString() {
        return "m_startAddress " + Address.toHexString(m_startAddress) + ", m_endAddress " +
                Address.toHexString(m_endAddress);
    }
}
