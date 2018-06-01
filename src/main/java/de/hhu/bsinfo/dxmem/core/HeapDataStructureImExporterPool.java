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

public final class HeapDataStructureImExporterPool {
    private final Heap m_heap;
    private final HeapDataStructureImExporter[] m_pool;

    public HeapDataStructureImExporterPool(final Heap p_heap) {
        m_heap = p_heap;
        m_pool = new HeapDataStructureImExporter[1024];
    }

    public HeapDataStructureImExporter get() {
        HeapDataStructureImExporter tmp = m_pool[(int) Thread.currentThread().getId()];

        if (tmp == null) {
            tmp = new HeapDataStructureImExporter(m_heap);
            m_pool[(int) Thread.currentThread().getId()] = tmp;
        }

        return tmp;
    }
}
