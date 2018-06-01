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

public class HeapArea {
    private long m_startAddress;
    private long m_endAddress;

    HeapArea(final long p_startAddress, final long p_endAddress) {
        m_startAddress = p_startAddress;
        m_endAddress = p_endAddress;
    }

    public long getStartAddress() {
        return m_startAddress;
    }

    public long getEndAddress() {
        return m_endAddress;
    }

    @Override
    public String toString() {
        return "m_startAddress " + Address.toHexString(m_startAddress) + ", m_endAddress " +
                Address.toHexString(m_endAddress);
    }
}
