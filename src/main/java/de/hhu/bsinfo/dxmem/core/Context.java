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
 * Wrapper object used to wrap various data structures used in operations
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Context {
    private final short m_nodeId;
    private final Heap m_heap;
    private final CIDTable m_cidTable;
    private final LIDStore m_lidStore;
    private final CIDTableEntryPool m_cidTableEntryPool;
    private final HeapDataStructureImExporterPool m_dataStructureImExporterPool;
    private final Defragmenter m_defragmenter;

    private final boolean m_disableChunkLock;

    /**
     * Constructor
     * Used when heap is loaded from a mem dump file
     *
     * @param p_memdumpFile
     *         Path to memory dump file
     * @param p_disableChunkLock
     *         Disable the chunk lock mechanism which increases performance but blocks the remove
     *         and resize operations. All lock operation arguments provided on operation calls are
     *         ignored. DXMem cannot guarantee application data consistency on parallel writes to
     *         the same chunk. Useful for read only applications or if the application handles
     *         synchronization when writing to chunks.
     */
    public Context(final String p_memdumpFile, final boolean p_disableChunkLock) {
        MemoryLoader loader = new MemoryLoader();
        loader.load(p_memdumpFile);

        m_heap = loader.getHeap();
        m_cidTable = loader.getCIDTable();
        m_lidStore = loader.getLIDStore();

        m_nodeId = m_cidTable.getOwnNodeId();
        m_cidTableEntryPool = new CIDTableEntryPool();

        m_dataStructureImExporterPool = new HeapDataStructureImExporterPool(m_heap);

        // TODO non implemented defragmenter disabled for now (hardcoded)
        m_defragmenter = new Defragmenter(false);

        m_disableChunkLock = p_disableChunkLock;
    }

    /**
     * Constructor
     *
     * @param p_ownNodeId
     *         Node id of current instance
     * @param p_sizeBytes
     *         Size of heap in bytes
     * @param p_disableChunkLock
     *         Disable the chunk lock mechanism which increases performance but blocks the remove
     *         and resize operations. All lock operation arguments provided on operation calls are
     *         ignored. DXMem cannot guarantee application data consistency on parallel writes to
     *         the same chunk. Useful for read only applications or if the application handles
     *         synchronization when writing to chunks.
     */
    public Context(final short p_ownNodeId, final long p_sizeBytes, final boolean p_disableChunkLock) {
        m_nodeId = p_ownNodeId;
        m_cidTableEntryPool = new CIDTableEntryPool();

        m_heap = new Heap(p_sizeBytes);
        m_dataStructureImExporterPool = new HeapDataStructureImExporterPool(m_heap);
        m_cidTable = new CIDTable(p_ownNodeId, m_heap);
        m_lidStore = new LIDStore(p_ownNodeId, m_cidTable);

        // TODO non implemented defragmenter disabled for now (hardcoded)
        m_defragmenter = new Defragmenter(false);

        m_disableChunkLock = p_disableChunkLock;
    }

    /**
     * Destroy the context
     */
    public void destroy() {
        m_heap.destroy();
    }

    /**
     * Get the node id of the current instance
     *
     * @return Node id
     */
    public short getNodeId() {
        return m_nodeId;
    }

    /**
     * Get the heap
     *
     * @return Heap
     */
    public Heap getHeap() {
        return m_heap;
    }

    /**
     * Get the CIDTable
     *
     * @return CIDTable
     */
    public CIDTable getCIDTable() {
        return m_cidTable;
    }

    /**
     * Get the LIDStore
     *
     * @return LIDStore
     */
    public LIDStore getLIDStore() {
        return m_lidStore;
    }

    /**
     * Get the CIDTableEntryPool
     *
     * @return CIDTableEntryPool
     */
    public CIDTableEntryPool getCIDTableEntryPool() {
        return m_cidTableEntryPool;
    }

    /**
     * Get the HeapDataStructureImExporterPool
     *
     * @return HeapDataStructureImExporterPool
     */
    public HeapDataStructureImExporterPool getDataStructureImExporterPool() {
        return m_dataStructureImExporterPool;
    }

    /**
     * Get the Defragmenter
     *
     * @return Defragmenter
     */
    public Defragmenter getDefragmenter() {
        return m_defragmenter;
    }

    /**
     * Chunk lock disabled flag
     *
     * @return True on disabled, false on enabled
     */
    public boolean isChunkLockDisabled() {
        return m_disableChunkLock;
    }
}
