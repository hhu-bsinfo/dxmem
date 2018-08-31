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

import java.io.File;
import java.io.FileNotFoundException;

import de.hhu.bsinfo.dxutils.serialization.RandomAccessFileImExporter;

/**
 * Helper class to load a memory dump from a file
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class MemoryLoader {
    private Heap m_heap;
    private CIDTable m_table;
    private LIDStore m_lidStore;

    /**
     * Constructor
     */
    public MemoryLoader() {
        m_heap = new Heap();
        m_table = new CIDTable();
        m_lidStore = new LIDStore();
    }

    /**
     * Load memory dump from a file
     *
     * @param p_file
     *         Path to file to load from
     */
    public void load(final String p_file) {
        assert p_file != null;

        File file = new File(p_file);

        if (!file.exists()) {
            throw new MemoryRuntimeException("Cannot load mem dump " + p_file + ": file does not exist");
        }

        m_table = new CIDTable();

        RandomAccessFileImExporter importer;

        try {
            importer = new RandomAccessFileImExporter(file);
        } catch (final FileNotFoundException e) {
            // cannot happen
            throw new MemoryRuntimeException("Illegal state", e);
        }

        importer.importObject(m_heap);
        importer.importObject(m_table);
        importer.importObject(m_lidStore);

        importer.close();

        m_heap = m_table.m_heap;
    }

    /**
     * Get the loaded heap instance
     *
     * @return Heap
     */
    public Heap getHeap() {
        return m_heap;
    }

    /**
     * Get the loaded CIDTable instance
     *
     * @return CIDTable
     */
    public CIDTable getCIDTable() {
        return m_table;
    }

    /**
     * Get the loaded LIDStore instance
     *
     * @return LIDStore
     */
    public LIDStore getLIDStore() {
        return m_lidStore;
    }
}
