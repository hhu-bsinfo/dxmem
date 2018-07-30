package de.hhu.bsinfo.dxmem.benchmark.operation;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkBenchmark;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.RandomUtils;

public class Get extends AbstractOperation {
    private final long m_cidRangeStart;
    private final long m_cidRangeEnd;
    private final int m_maxChunkSize;
    private final ChunkBenchmark[] m_chunks;

    public Get(final float p_probability, final int p_batchCount, final boolean p_verifyData,
            final long p_cidRangeStart, final long p_cidRangeEnd, final int p_maxChunkSize) {
        super("get", p_probability, p_batchCount, p_verifyData);

        m_cidRangeStart = p_cidRangeStart;
        m_cidRangeEnd = p_cidRangeEnd;
        m_maxChunkSize = p_maxChunkSize;

        m_chunks = new ChunkBenchmark[1024];
    }

    @Override
    public ChunkState execute(final DXMem p_memory, final boolean p_verifyData) {
        long cid = RandomUtils.getRandomValue(m_cidRangeStart, m_cidRangeEnd);

        int tid = (int) Thread.currentThread().getId();

        if (m_chunks[tid] == null) {
            m_chunks[tid] = new ChunkBenchmark(m_maxChunkSize);
        }

        m_chunks[tid].setID(cid);

        p_memory.get().get(m_chunks[tid]);

        // verify data in chunk
        if (p_verifyData) {
            if (!m_chunks[tid].verifyContents()) {
                return ChunkState.DATA_LOST;
            }
        }

        return m_chunks[tid].getState();
    }
}
