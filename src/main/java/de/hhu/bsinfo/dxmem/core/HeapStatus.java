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

import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Holds some state and information about the heap
 *
 * @author Florian Klein, florian.klein@hhu.de, 10.04.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 07.06.2018
 */
public final class HeapStatus implements Importable, Exportable {
    long m_totalSizeBytes;
    long m_freeSizeBytes;
    long m_allocatedPayloadBytes;
    long m_allocatedBlocks;
    long m_freeBlocks;
    long m_freeSmall64ByteBlocks;

    /**
     * Get the total size of the memory
     *
     * @return Total size in bytes
     */
    public long getTotalSizeBytes() {
        return m_totalSizeBytes;
    }

    /**
     * Get the total size of the memory
     *
     * @return Total size as StorageUnit
     */
    public StorageUnit getTotalSize() {
        return new StorageUnit(m_totalSizeBytes, StorageUnit.BYTE);
    }

    /**
     * Get the total amount of free memory
     *
     * @return Total free memory in bytes
     */
    public long getFreeSizeBytes() {
        return m_freeSizeBytes;
    }

    /**
     * Get the total amount of free memory
     *
     * @return Total free memory as StorageUnit
     */
    public StorageUnit getFreeSize() {
        return new StorageUnit(m_freeSizeBytes, StorageUnit.BYTE);
    }

    /**
     * Get the amount of used memory
     *
     * @return Amount of used memory in bytes
     */
    public long getUsedSizeBytes() {
        return m_totalSizeBytes - m_freeSizeBytes;
    }

    /**
     * Get the amount of used memory
     *
     * @return Amount of used memory as StorageUnit
     */
    public StorageUnit getUsedSize() {
        return new StorageUnit(getUsedSizeBytes(), StorageUnit.BYTE);
    }

    /**
     * Get the total amount of bytes used for payload of the allocated blocks
     *
     * @return Amount of memory in bytes used for payload
     */
    public long getAllocatedPayloadBytes() {
        return m_allocatedPayloadBytes;
    }

    /**
     * Get the total amount used for payload of the allocated blocks
     *
     * @return Amount of memory used for payload as StorageUnit
     */
    public StorageUnit getAllocatedPayload() {
        return new StorageUnit(m_allocatedPayloadBytes, StorageUnit.BYTE);
    }

    /**
     * Get the total number of allocated blocks
     *
     * @return Number of allocated blocks
     */
    public long getAllocatedBlocks() {
        return m_allocatedBlocks;
    }

    /**
     * Get the total number of free blocks
     *
     * @return Number of free blocks
     */
    public long getFreeBlocks() {
        return m_freeBlocks;
    }

    /**
     * Get the total number of free blocks with a size of less than 64 bytes
     *
     * @return Number of small free blocks
     */
    public long getFreeSmall64ByteBlocks() {
        return m_freeSmall64ByteBlocks;
    }

    /**
     * Gets the current fragmentation in percentage
     *
     * @return the fragmentation
     */
    public double getFragmentation() {
        double ret = 0;

        if (m_freeSmall64ByteBlocks >= 1 || m_freeBlocks >= 1) {
            ret = (double) m_freeSmall64ByteBlocks / m_freeBlocks;
        }

        return ret;
    }

    @Override
    public String toString() {
        return "Status [m_totalSizeBytes=" + m_totalSizeBytes + ", m_freeSizeBytes=" + m_freeSizeBytes +
                ", m_allocatedPayloadBytes=" + m_allocatedPayloadBytes + ", m_allocatedBlocks=" + m_allocatedBlocks +
                ", m_freeBlocks=" + m_freeBlocks + ", m_freeSmall64ByteBlocks=" + m_freeSmall64ByteBlocks +
                ", fragmentation=" + getFragmentation() + ']';
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_totalSizeBytes);
        p_exporter.writeLong(m_freeSizeBytes);
        p_exporter.writeLong(m_allocatedPayloadBytes);
        p_exporter.writeLong(m_allocatedBlocks);
        p_exporter.writeLong(m_freeBlocks);
        p_exporter.writeLong(m_freeSmall64ByteBlocks);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_totalSizeBytes = p_importer.readLong(m_totalSizeBytes);
        m_freeSizeBytes = p_importer.readLong(m_freeSizeBytes);
        m_allocatedPayloadBytes = p_importer.readLong(m_allocatedPayloadBytes);
        m_allocatedBlocks = p_importer.readLong(m_allocatedBlocks);
        m_freeBlocks = p_importer.readLong(m_freeBlocks);
        m_freeSmall64ByteBlocks = p_importer.readLong(m_freeSmall64ByteBlocks);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES * 6;
    }
}
