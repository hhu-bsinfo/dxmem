package de.hhu.bsinfo.dxmem.core;

import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class LIDStoreStatus implements Importable, Exportable {
    long m_currentLIDCounter;
    long m_totalFreeLIDs;
    int m_lidsInStore;

    public long getCurrentLIDCounter() {
        return m_currentLIDCounter;
    }

    public long getTotalFreeLIDs() {
        return m_totalFreeLIDs;
    }

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
