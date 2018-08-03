package de.hhu.bsinfo.dxmem.data;

import java.util.Random;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

// chunk with "variable size" used for memory benchmarking to avoid allocations if pooling is not possible
// can be adopted for actual applications as well (depending on the use case)
public class ChunkBenchmark extends AbstractChunk {
    private final byte[] m_data;
    private int m_currentSize;

    public ChunkBenchmark(final int p_maxBufferSize) {
        this(ChunkID.INVALID_ID, p_maxBufferSize);
    }

    public ChunkBenchmark(final long p_id, final int p_maxBufferSize) {
        super(p_id);

        m_data = new byte[p_maxBufferSize];
        m_currentSize = p_maxBufferSize;
    }

    public void setCurrentSize(final int p_size) {
        if (p_size > m_data.length) {
            throw new IllegalStateException("Invalid size " + p_size + " exceeding max size " + m_data.length);
        }

        m_currentSize = p_size;
    }

    public void fillContents() {
        Random rand = new Random(getID());

        for (int i = 0; i < m_currentSize; i++) {
            m_data[i] = (byte) rand.nextInt();
        }
    }

    public boolean verifyContents() {
        Random rand = new Random(getID());

        for (int i = 0; i < m_currentSize; i++) {
            if (m_data[i] != (byte) rand.nextInt()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.readBytes(m_data, 0, m_currentSize);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBytes(m_data, 0, m_currentSize);
    }

    @Override
    public int sizeofObject() {
        return m_currentSize;
    }
}
