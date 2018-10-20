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

import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * Tool to calculate memory overhead when storing data in DXMem
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class MemoryOverheadCalculator {
    private final int m_chunkPayloadSize;
    private final long m_totalChunkCount;
    private final StorageUnit m_totalPayloadMem;
    private final StorageUnit m_chunkSizeMemory;
    private final StorageUnit m_nidTableSize;
    private final StorageUnit[] m_lidTableSizes;
    private final StorageUnit m_totalMem;
    private final StorageUnit m_overheadMem;
    private final float m_overhead;

    /**
     * Constructor
     *
     * @param p_chunkPayloadSize
     *         Payload size of chunk
     * @param p_totalChunkCount
     *         Total number of chunks
     */
    public MemoryOverheadCalculator(final int p_chunkPayloadSize, final long p_totalChunkCount) {
        m_chunkPayloadSize = p_chunkPayloadSize;
        m_totalChunkCount = p_totalChunkCount;

        m_totalPayloadMem = new StorageUnit(p_totalChunkCount * p_chunkPayloadSize, "b");
        m_chunkSizeMemory = new StorageUnit(calcTotalChunkSizeMemory(p_chunkPayloadSize, p_totalChunkCount), "b");
        m_nidTableSize = new StorageUnit(calcSizeNIDTable(), "b");
        m_lidTableSizes = new StorageUnit[CIDTable.LID_TABLE_LEVELS];

        for (int i = 0; i < m_lidTableSizes.length; i++) {
            m_lidTableSizes[i] = new StorageUnit(calcSizeLIDTable(i, p_totalChunkCount), "b");
        }

        long tmp = 0;

        tmp += m_chunkSizeMemory.getBytes();
        tmp += m_nidTableSize.getBytes();

        for (StorageUnit lidTableSize : m_lidTableSizes) {
            tmp += lidTableSize.getBytes();
        }

        m_totalMem = new StorageUnit(tmp, "b");
        m_overheadMem = new StorageUnit(m_totalMem.getBytes() - m_totalPayloadMem.getBytes(), "b");
        m_overhead = ((float) m_overheadMem.getBytes() / m_totalMem.getBytes()) * 100.0f;
    }

    /**
     * Calculate the overhead based on HeapStatus and CIDTableStatus
     *
     * @param p_heapStatus
     *         HeapStatus instance
     * @param p_cidTableStatus
     *         CIDTableStatus instance
     * @return Memory metadata overhead (0.0 - 1.0)
     */
    public static double calculate(final HeapStatus p_heapStatus, final CIDTableStatus p_cidTableStatus) {
        long dataPayloadBytes = p_heapStatus.getAllocatedPayloadBytes() -
                p_cidTableStatus.getTotalPayloadMemoryTablesBytes();

        long metadataOverheadBytes = p_heapStatus.getUsedSizeBytes() - dataPayloadBytes;

        return (double) metadataOverheadBytes / p_heapStatus.getUsedSizeBytes();
    }

    /**
     * Get the chunk payload size
     *
     * @return Chunk payload size
     */
    public int getChunkPayloadSize() {
        return m_chunkPayloadSize;
    }

    /**
     * Get the total chunk count
     *
     * @return Total chunk count
     */
    public long getTotalChunkCount() {
        return m_totalChunkCount;
    }

    /**
     * Get the total chunk payload memory used
     *
     * @return Total chunk payload memory
     */
    public StorageUnit getTotalPayloadMem() {
        return m_totalPayloadMem;
    }

    /**
     * Get the total memory used for chunks
     *
     * @return Memory used for chunks
     */
    public StorageUnit getChunkSizeMemory() {
        return m_chunkSizeMemory;
    }

    /**
     * Get the memory used for NID tables
     *
     * @return Memory used for NID tables
     */
    public StorageUnit getNIDTableSize() {
        return m_nidTableSize;
    }

    /**
     * Get the memory used for LID tables
     *
     * @return Memory used for LID tables (per level)
     */
    public StorageUnit[] getLIDTableSizes() {
        return m_lidTableSizes;
    }

    /**
     * Get the total amount of memory used
     *
     * @return Total amount of memory used
     */
    public StorageUnit getTotalMem() {
        return m_totalMem;
    }

    /**
     * Get the total amount of overhead
     *
     * @return Total amount of overhead
     */
    public StorageUnit getOverheadMem() {
        return m_overheadMem;
    }

    /**
     * Get the memory metadata overhead
     *
     * @return Memory metadata overhead (0.0 - 1.0)
     */
    public float getOverhead() {
        return m_overhead;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("Chunk payload size: ");
        builder.append(new StorageUnit(m_chunkPayloadSize, "b"));
        builder.append('\n');
        builder.append("Total chunk count: ");
        builder.append(m_totalChunkCount);
        builder.append('\n');
        builder.append("-------------------");
        builder.append('\n');
        builder.append("Total chunks size (including marker and length field): ");
        builder.append(m_chunkSizeMemory);
        builder.append('\n');
        builder.append("Nid table size: ");
        builder.append(m_nidTableSize);
        builder.append('\n');

        for (int i = m_lidTableSizes.length - 1; i >= 0; i--) {
            builder.append("Lid tables level ");
            builder.append(i);
            builder.append(" total size: ");
            builder.append(m_lidTableSizes[i]);
            builder.append('\n');
        }

        builder.append("-------------------");
        builder.append('\n');
        builder.append("Total payload memory: ");
        builder.append(m_totalPayloadMem);
        builder.append(" (");
        builder.append(m_totalPayloadMem.getBytes());
        builder.append(")\n");
        builder.append("Total overhead only: ");
        builder.append(m_overheadMem);
        builder.append(" (");
        builder.append(m_overheadMem.getBytes());
        builder.append(")\n");
        builder.append("Total memory including overhead: ");
        builder.append(m_totalMem);
        builder.append(" (");
        builder.append(m_totalMem.getBytes());
        builder.append(")\n");
        builder.append("Overhead (%): ");
        builder.append(m_overhead);

        return builder.toString();
    }

    /**
     * Calculate the size of a NID table
     *
     * @return Size of NID table
     */
    private static long calcSizeNIDTable() {
        return (long) CIDTable.ENTRIES_PER_NID_LEVEL * CIDTable.ENTRY_SIZE;
    }

    /**
     * Calculate the size of a LID table
     *
     * @param p_tableLevel
     *         Table level
     * @param p_totalNumChunks
     *         Total number of chunks
     * @return Size of LID table
     */
    private static long calcSizeLIDTable(final int p_tableLevel, final long p_totalNumChunks) {
        // round up to full lid tables
        return (long) Math.ceil(p_totalNumChunks / Math.pow(2, 12 * (p_tableLevel + 1))) *
                CIDTable.ENTRIES_PER_LID_LEVEL * CIDTable.ENTRY_SIZE;
    }

    /**
     * Calculate the total chunk size
     *
     * @param p_chunkPayloadSize
     *         Payload size of chunk
     * @param p_totalChunkCount
     *         Total number of chunks
     * @return Total memory used for chunks
     */
    private static long calcTotalChunkSizeMemory(final int p_chunkPayloadSize, final long p_totalChunkCount) {
        return calcTotalChunkSizeMemory(p_chunkPayloadSize) * p_totalChunkCount;
    }

    /**
     * Calculate the total size for a single chunk
     *
     * @param p_chunkPayloadSize
     *         Chunk payload size
     * @return Total size used in memory for chunk
     */
    private static long calcTotalChunkSizeMemory(final int p_chunkPayloadSize) {
        return Heap.SIZE_MARKER_BYTE + CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(p_chunkPayloadSize) +
                p_chunkPayloadSize;
    }
}
