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

/**
 * Status object for LIDStore
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class LIDStoreStatus implements Importable, Exportable {
    long m_currentLIDCounter;
    long m_totalFreeLIDs;
    int m_lidsInStore;

    /**
     * Get the current state of the LID counter
     *
     * @return Current LID counter state
     */
    public long getCurrentLIDCounter() {
        return m_currentLIDCounter;
    }

    /**
     * Get total number of free LIDs (in store and zombies)
     *
     * @return Total number of free LIDs
     */
    public long getTotalFreeLIDs() {
        return m_totalFreeLIDs;
    }

    /**
     * Get the total number of LIDs in store
     *
     * @return Total number of LIDs in store
     */
    public int getTotalLIDsInStore() {
        return m_lidsInStore;
    }

    @Override
    public String toString() {
        return "Status[m_currentLIDCounter " + m_currentLIDCounter + ", m_totalFreeLIDs " + m_totalFreeLIDs +
                ", m_lidsInStore " + m_lidsInStore + ']';
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_currentLIDCounter);
        p_exporter.writeLong(m_totalFreeLIDs);
        p_exporter.writeInt(m_lidsInStore);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_currentLIDCounter = p_importer.readLong(m_currentLIDCounter);
        m_totalFreeLIDs = p_importer.readLong(m_totalFreeLIDs);
        m_lidsInStore = p_importer.readInt(m_lidsInStore);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES * 2 + Integer.BYTES;
    }
}
