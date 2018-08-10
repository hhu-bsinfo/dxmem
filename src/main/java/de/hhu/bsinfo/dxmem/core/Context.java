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

public class Context {
    private final short m_nodeId;
    private final Heap m_heap;
    private final CIDTable m_cidTable;
    private final LIDStore m_lidStore;
    private final CIDTranslationCache m_cidTranslationCache;
    private final CIDTableEntryPool m_cidTableEntryPool;
    private final HeapDataStructureImExporterPool m_dataStructureImExporterPool;
    private final Defragmenter m_defragmenter;

    public Context(final String p_memdumpFile) {
        MemoryLoader loader = new MemoryLoader();
        loader.load(p_memdumpFile);

        m_heap = loader.getHeap();
        m_cidTable = loader.getCIDTable();
        m_lidStore = loader.getLIDStore();

        m_nodeId = m_cidTable.getOwnNodeId();
        m_cidTranslationCache = m_cidTable.m_cidTranslationCache;
        m_cidTableEntryPool = new CIDTableEntryPool();

        m_dataStructureImExporterPool = new HeapDataStructureImExporterPool(m_heap);

        // TODO non implemented defragmenter disabled for now (hardcoded)
        m_defragmenter = new Defragmenter(false);
    }

    public Context(final short p_ownNodeId, final long p_sizeBytes) {
        m_nodeId = p_ownNodeId;
        m_cidTranslationCache = new CIDTranslationCache();
        m_cidTableEntryPool = new CIDTableEntryPool();

        m_heap = new Heap(p_sizeBytes);
        m_dataStructureImExporterPool = new HeapDataStructureImExporterPool(m_heap);
        m_cidTable = new CIDTable(p_ownNodeId, m_heap, m_cidTranslationCache);
        m_lidStore = new LIDStore(p_ownNodeId, m_cidTable);

        // TODO non implemented defragmenter disabled for now (hardcoded)
        m_defragmenter = new Defragmenter(false);
    }

    public void destroy() {
        m_heap.destroy();
    }

    public short getNodeId() {
        return m_nodeId;
    }

    public Heap getHeap() {
        return m_heap;
    }

    public CIDTable getCIDTable() {
        return m_cidTable;
    }

    public LIDStore getLIDStore() {
        return m_lidStore;
    }

    public CIDTableEntryPool getCIDTableEntryPool() {
        return m_cidTableEntryPool;
    }

    public HeapDataStructureImExporterPool getDataStructureImExporterPool() {
        return m_dataStructureImExporterPool;
    }

    public Defragmenter getDefragmenter() {
        return m_defragmenter;
    }
}
