package de.hhu.bsinfo.dxmem.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import de.hhu.bsinfo.dxutils.serialization.RandomAccessFileImExporter;

public class MemoryDumper {
    private final CIDTable m_table;

    public MemoryDumper(final CIDTable p_table) {
        m_table = p_table;
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

        exporter.exportObject(m_table);
        exporter.close();
    }
}
