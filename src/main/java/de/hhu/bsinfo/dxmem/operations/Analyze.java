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

import java.util.ArrayList;

import de.hhu.bsinfo.dxmem.core.Analyzer;
import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.CIDTableTableEntry;
import de.hhu.bsinfo.dxmem.core.CIDTableZombieEntry;
import de.hhu.bsinfo.dxmem.core.Context;

/**
 * Analyze the heap and check for errors
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Analyze {
    private final Context m_context;

    private Analyzer m_analyzer;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public Analyze(final Context p_context) {
        m_context = p_context;

        m_analyzer = new Analyzer(m_context.getHeap(), m_context.getCIDTable());
    }

    /**
     * Analyze the heap
     *
     * @return True if errors were detected, false if heap is ok
     */
    public boolean analyze() {
        return m_analyzer.analyze();
    }

    /**
     * Get the collected CIDTable table entries after analyzing
     *
     * @return List of collected CIDTable table entries
     */
    public ArrayList<CIDTableTableEntry> getCIDTableTableEntries() {
        return m_analyzer.getCIDTableTableEntries();
    }

    /**
     * Get the collected CIDTable chunk entries after analyzing
     *
     * @return List of collected CIDTable chunk entries
     */
    public ArrayList<CIDTableChunkEntry> getCIDTableChunkEntries() {
        return m_analyzer.getCIDTableChunkEntries();
    }

    /**
     * Get the collected CIDTable zombie entries after analyzing
     *
     * @return List of collected CIDTable zombie entries
     */
    public ArrayList<CIDTableZombieEntry> getCIDTableZombieEntries() {
        return m_analyzer.getCIDTableZombieEntries();
    }
}
