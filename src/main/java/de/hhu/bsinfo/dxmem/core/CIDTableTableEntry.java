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
 * Helper class for handling table entries (table levels 3-1) in the CIDTable. This class caches the entry read from
 * memory, only. Any changes applied are NOT automatically written back to memory.
 * Structure:
 * | address 43 bit |
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class CIDTableTableEntry {
    static final long RAW_VALUE_FREE = 0;

    private static final long BITS_ADDRESS = Address.WIDTH_BITS;

    private static final long MASK_ADDRESS = (long) Math.pow(2, BITS_ADDRESS) - 1;

    static final long OFFSET_ADDRESS = 0;

    // raw pointer to the address where the entry is stored
    private long m_pointer;
    private long m_address;

    /**
     * Constructor
     */
    CIDTableTableEntry() {

    }

    /**
     * Constructor
     *
     * @param p_pointer
     *         Pointer (address) to table entry
     * @param p_value
     *         Value of table entry
     */
    CIDTableTableEntry(final long p_pointer, final long p_value) {
        set(p_pointer, p_value);
    }

    /**
     * Clear the object
     */
    public void clear() {
        m_pointer = Address.INVALID;
        m_address = Address.INVALID;
    }

    /**
     * Set the object. Extracts fields from raw entry
     *
     * @param p_pointer
     *         Pointer (address) of table entry
     * @param p_value
     *         Value of table entry
     */
    public void set(final long p_pointer, final long p_value) {
        m_pointer = p_pointer;
        m_address = p_value >> OFFSET_ADDRESS & MASK_ADDRESS;
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
     * Set the pointer
     *
     * @param p_pointer
     *         Pointer to entry
     */
    public void setPointer(final long p_pointer) {
        m_pointer = p_pointer;
    }

    /**
     * Get the value. Assembles all fields to fit into a single long
     *
     * @return Value
     */
    public long getValue() {
        return m_address << OFFSET_ADDRESS & MASK_ADDRESS;
    }

    /**
     * Check if the address is valid
     *
     * @return True if valid, false otherwise
     */
    public boolean isAddressValid() {
        return m_address != Address.INVALID;
    }

    /**
     * Get the address to the table
     *
     * @return Address to the table
     */
    public long getAddress() {
        return m_address;
    }

    /**
     * Set the address to the table
     *
     * @param p_value
     *         Address to set
     */
    public void setAddress(final long p_value) {
        assert p_value >= 0 && p_value <= MASK_ADDRESS;

        m_address = p_value;
    }

    /**
     * Get the address part of a table entry from a table
     *
     * @param p_rawValue
     *         Raw table entry value from a table
     * @return Address part (to table)
     */
    public static long getAddressOfRawTableEntry(final long p_rawValue) {
        return p_rawValue >> OFFSET_ADDRESS & MASK_ADDRESS;
    }

    @Override
    public String toString() {
        return "m_pointer " + Address.toHexString(m_pointer) + ", m_address " + Address.toHexString(m_address);
    }
}
