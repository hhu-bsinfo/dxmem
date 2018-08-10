package de.hhu.bsinfo.dxmem.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import de.hhu.bsinfo.dxutils.serialization.RandomAccessFileImExporter;

public class MemoryDumper {
    private final Heap m_heap;
    private final CIDTable m_table;
    private final LIDStore m_lidStore;

    public MemoryDumper(final Heap p_heap, final CIDTable p_table, final LIDStore p_lidStore) {
        m_heap = p_heap;
        m_table = p_table;
        m_lidStore = p_lidStore;
    }

    public void dump(final String p_outputFile) {
        assert p_outputFile != null;

        File file = new File(p_outputFile);

        if (file.exists()) {
            if (!file.delete()) {
                throw new MemoryRuntimeException("Deleting existing file for memory dump failed");
            }
        } else {
            try {
                if (!file.createNewFile()) {
                    throw new MemoryRuntimeException("Creating file for memory dump failed");
                }
            } catch (final IOException e) {
                throw new MemoryRuntimeException("Creating file for memory dump failed", e);
            }
        }

        RandomAccessFileImExporter exporter;

        try {
            exporter = new RandomAccessFileImExporter(file);
        } catch (final FileNotFoundException e) {
            // not possible
            throw new MemoryRuntimeException("Illegal state", e);
        }

        exporter.exportObject(m_heap);
        exporter.exportObject(m_table);
        exporter.exportObject(m_lidStore);

        exporter.close();
    }
}
