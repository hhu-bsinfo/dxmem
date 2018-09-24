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
 * Methods to raw write a chunk in the heap by using the address (chunk must be pinned in order to get
 * the address, first)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public class RawWrite {
    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public RawWrite(final Context p_context) {
        m_context = p_context;
    }

    public void copyNative(final long p_address, final int p_addressOffset, final long p_addressSource,
            final int p_offset, final int p_length, final boolean p_isAddressSourceAbsolute) {
        m_context.getHeap()
                .copyNative(p_address, p_addressOffset, p_addressSource, p_offset, p_length, p_isAddressSourceAbsolute);
    }

    public void writeByte(final long p_address, final int p_addressOffset, final byte p_value) {
        m_context.getHeap().writeByte(p_address, p_addressOffset, p_value);
    }

    public void writeShort(final long p_address, final int p_addressOffset, final short p_value) {
        m_context.getHeap().writeShort(p_address, p_addressOffset, p_value);
    }

    public void writeChar(final long p_address, final int p_addressOffset, final char p_value) {
        m_context.getHeap().writeChar(p_address, p_addressOffset, p_value);
    }

    public void writeInt(final long p_address, final int p_addressOffset, final int p_value) {
        m_context.getHeap().writeInt(p_address, p_addressOffset, p_value);
    }

    public void writeLong(final long p_address, final int p_addressOffset, final long p_value) {
        m_context.getHeap().writeLong(p_address, p_addressOffset, p_value);
    }

    public void write(final long p_address, final int p_addressOffset, final byte[] p_array) {
        write(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void write(final long p_address, final int p_addressOffset, final short[] p_array) {
        write(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void write(final long p_address, final int p_addressOffset, final char[] p_array) {
        write(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void write(final long p_address, final int p_addressOffset, final int[] p_array) {
        write(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void write(final long p_address, final int p_addressOffset, final long[] p_array) {
        write(p_address, p_addressOffset, p_array, 0, p_array.length);
    }

    public void write(final long p_address, final int p_addressOffset, final byte[] p_array, final int p_offset,
            final int p_length) {
        m_context.getHeap().writeBytes(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void write(final long p_address, final int p_addressOffset, final short[] p_array, final int p_offset,
            final int p_length) {
        m_context.getHeap().writeShorts(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void write(final long p_address, final int p_addressOffset, final char[] p_array, final int p_offset,
            final int p_length) {
        m_context.getHeap().writeChars(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void write(final long p_address, final int p_addressOffset, final int[] p_array, final int p_offset,
            final int p_length) {
        m_context.getHeap().writeInts(p_address, p_addressOffset, p_array, p_offset, p_length);
    }

    public void write(final long p_address, final int p_addressOffset, final long[] p_array, final int p_offset,
            final int p_length) {
        m_context.getHeap().writeLongs(p_address, p_addressOffset, p_array, p_offset, p_length);
    }
}
