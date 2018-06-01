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

public class CIDTableStatus implements Importable, Exportable {
    int m_totalTableCount;
    int[] m_tableCountLevel = new int[CIDTable.LID_TABLE_LEVELS];
    long m_totalPayloadMemoryTables;

    /**
     * Get the number of tables currently allocated.
     *
     * @return Number of tables currently allocated.
     */
    public int getTotalTableCount() {
        return m_totalTableCount;
    }

    /**
     * Get the number of tables allocated for a specific level
     *
     * @param p_level Table level to get number of allocated tables
     * @return Number of allocated tables for the specified level
     */
    public int getTableCountOfLevel(final int p_level) {
        if (p_level > 5) {
            throw new IllegalArgumentException("Invalid level specified: " + p_level);
        } else if (p_level == 5) {
            return 1;
        } else {
            return m_tableCountLevel[p_level];
        }
    }

    /**
     * Get the total amount of memory used by the tables.
     *
     * @return Amount of memory used by the tables (in bytes)
     */
    public long getTotalPayloadMemoryTables() {
        return m_totalPayloadMemoryTables;
    }

    @Override
    public String toString() {
        return "Status[m_totalTableCount=" + m_totalTableCount + ", m_tableCountLevel[3]=" + m_tableCountLevel[3] +
                ", m_tableCountLevel[2]=" + m_tableCountLevel[2] + ", m_tableCountLevel[1]=" + m_tableCountLevel[1] +
                ", m_tableCountLevel[0]=" + m_tableCountLevel[0] + ", m_totalPayloadMemoryTables=" +
                m_totalPayloadMemoryTables + ']';
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_totalTableCount);
        p_exporter.writeInt(m_tableCountLevel[3]);
        p_exporter.writeInt(m_tableCountLevel[2]);
        p_exporter.writeInt(m_tableCountLevel[1]);
        p_exporter.writeInt(m_tableCountLevel[0]);
        p_exporter.writeLong(m_totalPayloadMemoryTables);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_totalTableCount = p_importer.readInt(m_totalTableCount);
        m_tableCountLevel[3] = p_importer.readInt(m_tableCountLevel[3]);
        m_tableCountLevel[2] = p_importer.readInt(m_tableCountLevel[2]);
        m_tableCountLevel[1] = p_importer.readInt(m_tableCountLevel[1]);
        m_tableCountLevel[0] = p_importer.readInt(m_tableCountLevel[0]);
        m_totalPayloadMemoryTables = p_importer.readLong(m_totalPayloadMemoryTables);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 5 + Long.BYTES;
    }
}
