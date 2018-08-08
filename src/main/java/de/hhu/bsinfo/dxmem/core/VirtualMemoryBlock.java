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

import de.hhu.bsinfo.dxutils.UnsafeMemory;

/**
 * Wrapper to access Unsafe memory for the heap implementation
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 * @author Florian Hucke, florian.hucke@hhu.de, 08.02.2018
 */
public class VirtualMemoryBlock {
    private long m_memoryBase = -1;
    private long m_memorySize = -1;

    /**
     * Allocate/Initialize the VMB.
     * Make sure to call this before calling any other methods.
     *
     * @param p_size
     *         Size of the VMB in bytes.
     */
    public void allocate(final long p_size) {
        assert p_size > 0;

        try {
            m_memoryBase = UnsafeMemory.allocate(p_size);
        } catch (final Throwable e) {
            throw new MemoryRuntimeException("Could not initialize memory", e);
        }

        m_memorySize = p_size;
    }

    /**
     * Check if the VMB is allocated
     *
     * @return True if allocated, false otherwise
     */
    public boolean isAllocated() {
        return m_memoryBase != -1;
    }

    /**
     * Free/Cleanup the VMB.
     * Make sure to call this before object destruction.
     */
    public void free() {
        if (m_memoryBase == -1) {
            throw new IllegalStateException("Not allocated");
        }

        try {
            UnsafeMemory.free(m_memoryBase);
        } catch (final Throwable e) {
            throw new MemoryRuntimeException("Could not free memory", e);
        }

        m_memorySize = 0;
    }

    /**
     * Get the total allocated size of the VMB.
     *
     * @return Size of the VMB.
     */
    public long getSize() {
        return m_memorySize;
    }

    /**
     * Set a range of memory to a specified value.
     *
     * @param p_ptr
     *         Pointer to the start location.
     * @param p_size
     *         Number of bytes of the range.
     * @param p_value
     *         Value to set for specified range.
     */
    public void set(final long p_ptr, final long p_size, final byte p_value) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES * p_size);

        UnsafeMemory.set(m_memoryBase + p_ptr, p_size, p_value);
    }

    /**
     * Copy from a source native memory region to a target native memory region
     *
     * @param p_address
     *         Target address to copy to
     * @param p_addressOffset
     *         Offset in the target address to start at
     * @param p_addressSource
     *         Native memory address of the data to copy
     * @param p_offset
     *         Offset to start in the source data
     * @param p_length
     *         Number of bytes to copy from the source
     */
    public void copyNative(final long p_address, final int p_addressOffset, final long p_addressSource,
            final int p_offset, final int p_length) {
        assert assertMemoryBounds(p_address + p_offset, p_length);

        UnsafeMemory.copyBytes(p_addressSource + p_offset, p_address + p_addressOffset, p_length);
    }

    /**
     * Read data from the VMB into a byte array.
     *
     * @param p_ptr
     *         Start position in VMB.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the bytes to.
     * @param p_length
     *         Number of bytes to read from specified start.
     * @return Number of read elements.
     */
    public int readBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES * p_length);

        return UnsafeMemory.readBytes(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    /**
     * Read data from the VMB into a short array.
     *
     * @param p_ptr
     *         Start position in VMB.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the shorts to.
     * @param p_length
     *         Number of shorts to read from specified start.
     * @return Number of read elements.
     */
    public int readShorts(final long p_ptr, final short[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Short.BYTES * p_length);

        return UnsafeMemory.readShorts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    /**
     * Read data from the VMB into a char array.
     *
     * @param p_ptr
     *         Start position in VMB.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the chars to.
     * @param p_length
     *         Number of chars to read from specified start.
     * @return Number of read elements.
     */
    public int readChars(final long p_ptr, final char[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Character.BYTES * p_length);

        return UnsafeMemory.readChars(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    /**
     * Read data from the VMB into an int array.
     *
     * @param p_ptr
     *         Start position in VMB.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the ints to.
     * @param p_length
     *         Number of ints to read from specified start.
     * @return Number of read elements.
     */
    public int readInts(final long p_ptr, final int[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES * p_length);

        return UnsafeMemory.readInts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    /**
     * Read data from the VMB into a long array.
     *
     * @param p_ptr
     *         Start position in VMB.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the longs to.
     * @param p_length
     *         Number of longs to read from specified start.
     * @return Number of read elements.
     */
    public int readLongs(final long p_ptr, final long[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Long.BYTES * p_length);

        return UnsafeMemory.readLongs(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    /**
     * Read a single byte value.
     *
     * @param p_ptr
     *         Position to read from.
     * @return Byte read.
     */
    public byte readByte(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES);

        return UnsafeMemory.readByte(m_memoryBase + p_ptr);
    }

    /**
     * Read a single short value.
     *
     * @param p_ptr
     *         Position to read from.
     * @return Short read.
     */
    public short readShort(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Short.BYTES);

        return UnsafeMemory.readShort(m_memoryBase + p_ptr);
    }

    /**
     * Read a single char value.
     *
     * @param p_ptr
     *         Position to read from.
     * @return Char read.
     */
    public char readChar(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Character.BYTES);

        return UnsafeMemory.readChar(m_memoryBase + p_ptr);
    }

    /**
     * Read a single int value.
     *
     * @param p_ptr
     *         Position to read from.
     * @return Int read.
     */
    public int readInt(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES);

        return UnsafeMemory.readInt(m_memoryBase + p_ptr);
    }

    /**
     * Read a single long value.
     *
     * @param p_ptr
     *         Position to read from.
     * @return Long read.
     */
    public long readLong(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Long.BYTES);

        return UnsafeMemory.readLong(m_memoryBase + p_ptr);
    }

    /**
     * Write an array of bytes to the VMB.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public int writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES * p_length);

        return UnsafeMemory.writeBytes(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    /**
     * Write an array of shorts to the VMB.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public int writeShorts(final long p_ptr, final short[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Short.BYTES * p_length);

        return UnsafeMemory.writeShorts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    /**
     * Write an array of chars to the VMB.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public int writeChars(final long p_ptr, final char[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Character.BYTES * p_length);

        return UnsafeMemory.writeChars(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    /**
     * Write an array of ints to the VMB.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public int writeInts(final long p_ptr, final int[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES * p_length);

        return UnsafeMemory.writeInts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    /**
     * Write an array of longs to the VMB.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public int writeLongs(final long p_ptr, final long[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Long.BYTES * p_length);

        return UnsafeMemory.writeLongs(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    /**
     * Write a single byte value to the VMB.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public void writeByte(final long p_ptr, final byte p_value) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES);

        UnsafeMemory.writeByte(m_memoryBase + p_ptr, p_value);
    }

    /**
     * Write a single short value to the VMB.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public void writeShort(final long p_ptr, final short p_value) {
        assert assertMemoryBounds(p_ptr, Short.BYTES);

        UnsafeMemory.writeShort(m_memoryBase + p_ptr, p_value);
    }

    /**
     * Write a single char value to the VMB.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public void writeChar(final long p_ptr, final char p_value) {
        assert assertMemoryBounds(p_ptr, Character.BYTES);

        UnsafeMemory.writeChar(m_memoryBase + p_ptr, p_value);
    }

    /**
     * Write a single int value to the VMB.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public void writeInt(final long p_ptr, final int p_value) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES);

        UnsafeMemory.writeInt(m_memoryBase + p_ptr, p_value);
    }

    /**
     * Write a single long value to the VMB.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public void writeLong(final long p_ptr, final long p_value) {
        assert assertMemoryBounds(p_ptr, Long.BYTES);

        UnsafeMemory.writeLong(m_memoryBase + p_ptr, p_value);
    }

    /**
     * Read a value with specified number of bytes length from the VMB.
     *
     * @param p_ptr
     *         Address to read from.
     * @param p_count
     *         Number of bytes the value is stored to.
     * @return Value read.
     */
    public long readVal(final long p_ptr, final int p_count) {
        assert assertMemoryBounds(p_ptr, p_count);

        long val = 0;

        for (int i = 0; i < p_count; i++) {
            // kill the sign by & 0xFF
            val |= (long) (UnsafeMemory.readByte(m_memoryBase + p_ptr + i) & 0xFF) << 8 * i;
        }

        return val;
    }

    /**
     * Write a value with specified number of bytes length to the VMB.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_val
     *         Value to write.
     * @param p_count
     *         Number of bytes the value should occupy.
     */
    public void writeVal(final long p_ptr, final long p_val, final int p_count) {
        assert assertMemoryBounds(p_ptr, p_count);

        for (int i = 0; i < p_count; i++) {
            UnsafeMemory.writeByte(m_memoryBase + p_ptr + i, (byte) (p_val >> 8 * i & 0xFF));
        }
    }

    /**
     * Atomic CAS operation for a long value
     *
     * @param p_ptr
     *         Pointer to address of long
     * @param p_expectedValue
     *         Expected long value at address
     * @param p_newValue
     *         New value to swap if expected value matches
     * @return True if CAS operation successful, false if actual value different than expected
     */
    public boolean compareAndSwapLong(final long p_ptr, final long p_expectedValue, final long p_newValue) {
        return UnsafeMemory.compareAndSwapLong(m_memoryBase + p_ptr, p_expectedValue, p_newValue);
    }

    @Override
    public String toString() {
        return "m_memoryBase=0x" + Long.toHexString(m_memoryBase) + ", m_memorySize: " + m_memorySize;
    }

    /**
     * Check memory bounds on access
     *
     * @param p_ptr
     *         Ptr where to access
     * @param p_length
     *         Length of access
     * @return True if access ok, false on out of bounds
     */
    private boolean assertMemoryBounds(final long p_ptr, final long p_length) {
        if (p_ptr < 0) {
            throw new MemoryRuntimeException("Pointer is negative " + p_ptr);
        }

        if (p_ptr + p_length > m_memorySize || p_ptr + p_length < 0) {
            throw new MemoryRuntimeException("Accessing memory at " + p_ptr + ", length " + p_length +
                    " out of bounds: base " + m_memoryBase + ", size " + m_memorySize);
        }

        return true;
    }
}
