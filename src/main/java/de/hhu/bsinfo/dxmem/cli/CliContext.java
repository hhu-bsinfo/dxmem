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

package de.hhu.bsinfo.dxmem.cli;

import de.hhu.bsinfo.dxmem.DXMem;

public class CliContext {
    private final static CliContext ms_instance = new CliContext();

    private DXMem m_memory;

    // singletons are bad but having to use a dependency injection framework
    // for a single object is overkill
    public static CliContext getInstance() {
        return ms_instance;
    }

    private CliContext() {

    }

    public boolean isMemoryLoaded() {
        return m_memory != null;
    }

    public void newMemory(final short p_nodeId, final long p_heapSize) {
        if (m_memory != null) {
            m_memory.shutdown();
        }

        m_memory = new DXMem(p_nodeId, p_heapSize);
    }

    public void loadFromFile(final String p_inFile) {
        if (m_memory != null) {
            m_memory.shutdown();
        }

        m_memory = new DXMem(p_inFile);
    }

    public DXMem getMemory() {
        return m_memory;
    }
}
