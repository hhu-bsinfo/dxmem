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

    public static void main(final String[] p_args) {
        if (p_args.length < 2) {
            System.out.println("Calculate the required space and overhead of the memory manager");
            System.out.println("Usage: <chunkPayloadSize> <totalChunkCount>");
            return;
        }

        MemoryOverheadCalculator calc = new MemoryOverheadCalculator(Integer.parseInt(p_args[0]),
                Long.parseLong(p_args[1]));
        System.out.println(calc);
    }

    public static double calculate(final HeapStatus p_heapStatus, final CIDTableStatus p_cidTableStatus) {
        long dataPayloadBytes = p_heapStatus.getAllocatedPayloadBytes() -
                p_cidTableStatus.getTotalPayloadMemoryTablesBytes();

        long metadataOverheadBytes = p_heapStatus.getUsedSizeBytes() - dataPayloadBytes;

        return (double) metadataOverheadBytes / p_heapStatus.getUsedSizeBytes();
    }

    public int getChunkPayloadSize() {
        return m_chunkPayloadSize;
    }

    public long getTotalChunkCount() {
        return m_totalChunkCount;
    }

    public StorageUnit getTotalPayloadMem() {
        return m_totalPayloadMem;
    }

    public StorageUnit getChunkSizeMemory() {
        return m_chunkSizeMemory;
    }

    public StorageUnit getNIDTableSize() {
        return m_nidTableSize;
    }

    public StorageUnit[] getLIDTableSizes() {
        return m_lidTableSizes;
    }

    public StorageUnit getTotalMem() {
        return m_totalMem;
    }

    public StorageUnit getOverheadMem() {
        return m_overheadMem;
    }

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

        for (int i = 0; i < m_lidTableSizes.length; i++) {
            builder.append("Lid table level ");
            builder.append(i);
            builder.append(" size: ");
            builder.append(m_lidTableSizes[i]);
            builder.append('\n');
        }

        builder.append("-------------------");
        builder.append('\n');
        builder.append("Total payload memory: ");
        builder.append(m_totalPayloadMem);
        builder.append('\n');
        builder.append("Total overhead only: ");
        builder.append(m_overheadMem);
        builder.append('\n');
        builder.append("Total memory including overhead: ");
        builder.append(m_totalMem);
        builder.append('\n');
        builder.append("Overhead (%): ");
        builder.append(m_overhead);

        return builder.toString();
    }

    private static long calcSizeNIDTable() {
        return (long) CIDTable.ENTRIES_PER_NID_LEVEL * CIDTable.ENTRY_SIZE;
    }

    private static long calcSizeLIDTable(final int p_tableLevel, final long p_totalNumChunks) {
        // round up to full lid tables
        return (long) Math.ceil(p_totalNumChunks / Math.pow(2, 12 * (p_tableLevel + 1))) *
                CIDTable.ENTRIES_PER_LID_LEVEL * CIDTable.ENTRY_SIZE;
    }

    private static long calcTotalChunkSizeMemory(final int p_chunkPayloadSize, final long p_totalChunkCount) {
        return calcTotalChunkSizeMemory(p_chunkPayloadSize) * p_totalChunkCount;
    }

    private static long calcTotalChunkSizeMemory(final int p_chunkPayloadSize) {
        return Heap.SIZE_MARKER_BYTE + CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(p_chunkPayloadSize) +
                p_chunkPayloadSize;
    }
}
