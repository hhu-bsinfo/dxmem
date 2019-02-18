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

package de.hhu.bsinfo.dxmem.data;

import java.security.InvalidParameterException;
import java.util.Arrays;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Implementation of a chunk based on a generic byte array
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public final class ChunkByteArray extends AbstractChunk {
    private byte[] m_data;

    /**
     * Constructor
     */
    public ChunkByteArray() {

    }

    /**
     * Constructor
     * Sets the chunk id to invalid.
     *
     * @param p_bufferSize
     *         p_bufferSize Initial size of the byte array
     */
    public ChunkByteArray(final int p_bufferSize) {
        super();
        m_data = new byte[p_bufferSize];
    }

    /**
     * Constructor
     *
     * @param p_id
     *         ID the chunk is assigned to.
     * @param p_bufferSize
     *         Initial size of the byte array
     */
    public ChunkByteArray(final long p_id, final int p_bufferSize) {
        super(p_id);
        m_data = new byte[p_bufferSize];
    }

    /**
     * Constructor
     * Sets the chunk id to invalid.
     * Create the chunk with an external buffer.
     *
     * @param p_buffer
     *         External buffer containing the data for the chunk. Be careful
     *         with shared references.
     */
    public ChunkByteArray(final byte[] p_buffer) {
        super();

        if (p_buffer == null) {
            throw new InvalidParameterException("p_buffer == null");
        }

        m_data = p_buffer;
    }

    /**
     * Constructor
     * Create the chunk with an external buffer.
     *
     * @param p_id
     *         ID the chunk is assigned to.
     * @param p_buffer
     *         External buffer containing the data for the chunk. Be careful
     *         with shared references.
     */
    public ChunkByteArray(final long p_id, final byte[] p_buffer) {
        super(p_id);

        if (p_buffer == null) {
            throw new InvalidParameterException("p_buffer == null");
        }

        m_data = p_buffer;
    }

    /**
     * Gets the underlying byte buffer with the stored payload.
     * Note: The position gets reset to 0 before returning the reference.
     *
     * @return ByteBuffer with position reset.
     */
    public byte[] getData() {
        return m_data;
    }

    /**
     * Get the size of the data
     *
     * @return Data size in bytes
     */
    public int getSize() {
        return m_data.length;
    }

    @Override
    public String toString() {
        return "ChunkByteBuffer[" + ChunkID.toHexString(getID()) + ", " + getState() + ", " + m_data.length + ']';
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.readBytes(m_data);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBytes(m_data);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofByteArray(m_data);
    }

    @Override
    public boolean equals(final Object p_other) {
        if (p_other instanceof ChunkByteArray) {
            ChunkByteArray other = (ChunkByteArray) p_other;

            return Arrays.equals(m_data, other.m_data);
        } else {
            return false;
        }
    }
}
