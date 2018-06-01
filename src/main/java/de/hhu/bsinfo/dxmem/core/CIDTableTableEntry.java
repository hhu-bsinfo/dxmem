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

// | address 43 bit |
public class CIDTableTableEntry {
    static final long RAW_VALUE_FREE = 0;

    private static final long BITS_ADDRESS = Address.WIDTH_BITS;

    private static final long MASK_ADDRESS = (long) Math.pow(2, BITS_ADDRESS) - 1;

    static final long OFFSET_ADDRESS = 0;

    // raw pointer to the address where the entry is stored
    private long m_pointer;
    private long m_address;

    CIDTableTableEntry() {

    }

    CIDTableTableEntry(final long p_pointer, final long p_value) {
        set(p_pointer, p_value);
    }

    public void clear() {
        m_pointer = Address.INVALID;
        m_address = Address.INVALID;
    }

    public void set(final long p_pointer, final long p_value) {
        m_pointer = p_pointer;
        m_address = p_value >> OFFSET_ADDRESS & MASK_ADDRESS;
    }

    public long getPointer() {
        return m_pointer;
    }

    public void setPointer(final long p_pointer) {
        m_pointer = p_pointer;
    }

    public long getValue() {
        return m_address << OFFSET_ADDRESS & MASK_ADDRESS;
    }

    public boolean isAddressValid() {
        return m_address != Address.INVALID;
    }

    public long getAddress() {
        return m_address;
    }

    public void setAddress(final long p_value) {
        assert p_value >= 0 && p_value <= MASK_ADDRESS;

        m_address = p_value;
    }

    public static long getAddressOfRawTableEntry(final long p_rawValue) {
        return p_rawValue >> OFFSET_ADDRESS & MASK_ADDRESS;
    }

    @Override
    public String toString() {
        return "m_pointer " + Address.toHexString(m_pointer) +  ", m_address " + Address.toHexString(m_address);
    }
}
