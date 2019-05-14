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

package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.core.Context;

/**
 * Methods to raw read/access a chunk in the heap by using the address (chunk must be pinned in order to get
 * the address, first)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 14.03.2019
 */
public class RawRead {
    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context Context
     */
    public RawRead(final Context p_context) {
        m_context = p_context;
    }

    public boolean readBoolean(final long p_address, final int p_addressOffset) {
        return m_context.getHeap().readBoolean(p_address, p_addressOffset);
    }

    public byte readByte(final long p_address, final int p_addressOffset) {
        return m_context.getHeap().readByte(p_address, p_addressOffset);
    }

    public short readShort(final long p_address, final int p_addressOffset) {
        return m_context.getHeap().readShort(p_address, p_addressOffset);
    }

    public char readChar(final long p_address, final int p_addressOffset) {
        return m_context.getHeap().readChar(p_address, p_addressOffset);
    }

    public int readInt(final long p_address, final int p_addressOffset) {
        return m_context.getHeap().readInt(p_address, p_addressOffset);
    }

    public long readLong(final long p_address, final int p_addressOffset) {
        return m_context.getHeap().readLong(p_address, p_addressOffset);
    }

    public double readDouble(final long p_address, final int p_addressOffset) {
        return m_context.getHeap().readDouble(p_address, p_addressOffset);
    }

    public float readFloat(final long p_address, final int p_addressOffset) {
        return m_context.getHeap().readFloat(p_address, p_addressOffset);
    }

    public byte[] readByteArray(final long p_address, final int p_addressOffset, final int p_length) {
        byte[] arr = new byte[p_length];
        read(p_address, p_addressOffset, arr, 0, p_length);
        return arr;
    }

    public void read(final long p_address, final int p_addressOffset, final boolean[] p_array) {
        read(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void read(final long p_address, final int p_addressOffset, final byte[] p_array) {
        read(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void read(final long p_address, final int p_addressOffset, final short[] p_array) {
        read(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void read(final long p_address, final int p_addressOffset, final char[] p_array) {
        read(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void read(final long p_address, final int p_addressOffset, final int[] p_array) {
        read(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void read(final long p_address, final int p_addressOffset, final long[] p_array) {
        read(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void read(final long p_address, final int p_addressOffset, final double[] p_array) {
        read(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void read(final long p_address, final int p_addressOffset, final float[] p_array) {
        read(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void read(final long p_address, final int p_addressOffset, final boolean[] p_array, final int p_offset,
                     final int p_length) {
        m_context.getHeap().readBooleans(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void read(final long p_address, final int p_addressOffset, final byte[] p_array, final int p_offset,
                     final int p_length) {
        m_context.getHeap().readBytes(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void read(final long p_address, final int p_addressOffset, final short[] p_array, final int p_offset,
                     final int p_length) {
        m_context.getHeap().readShorts(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void read(final long p_address, final int p_addressOffset, final char[] p_array, final int p_offset,
                     final int p_length) {
        m_context.getHeap().readChars(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void read(final long p_address, final int p_addressOffset, final int[] p_array, final int p_offset,
                     final int p_length) {
        m_context.getHeap().readInts(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void read(final long p_address, final int p_addressOffset, final long[] p_array, final int p_offset,
                     final int p_length) {
        m_context.getHeap().readLongs(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void read(final long p_address, final int p_addressOffset, final double[] p_array, final int p_offset,
                     final int p_length) {
        m_context.getHeap().readDoubles(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void read(final long p_address, final int p_addressOffset, final float[] p_array, final int p_offset,
                     final int p_length) {
        m_context.getHeap().readFloats(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

}
