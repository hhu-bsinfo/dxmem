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

/**
 * Wrapper class which is necessary to access the memory instance in various places. This solutions is quite ugly
 * but I haven't found a way to allow passing some sort of context along with the picocli's commands.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class CliContext {
    private static final CliContext INSTANCE = new CliContext();

    private DXMem m_memory;

    /**
     * Singletons are bad but having to use a dependency injection framework
     * for a single object is overkill
     *
     * @return Singleton
     */
    public static CliContext getInstance() {
        return INSTANCE;
    }

    /**
     * Singleton class
     */
    private CliContext() {

    }

    /**
     * Check if memory is loaded
     *
     * @return True if loaded, false otherwise
     */
    public boolean isMemoryLoaded() {
        return m_memory != null;
    }

    /**
     * Create a new memory instance
     *
     * @param p_nodeId
     *         Node id to use for instance
     * @param p_heapSize
     *         Size of heap in bytes
     */
    public void newMemory(final short p_nodeId, final long p_heapSize, final boolean p_disableChunkLocks) {
        if (m_memory != null) {
            m_memory.shutdown();
        }

        m_memory = new DXMem(p_nodeId, p_heapSize, p_disableChunkLocks);
    }

    /**
     * Load the heap from a file
     *
     * @param p_inFile
     *         File to load heap from
     */
    public void loadFromFile(final String p_inFile, final boolean p_disableChunkLocks) {
        if (m_memory != null) {
            m_memory.shutdown();
        }

        m_memory = new DXMem(p_inFile, p_disableChunkLocks);
    }

    /**
     * Get the memory instance
     *
     * @return DXMem instance
     */
    public DXMem getMemory() {
        return m_memory;
    }
}
