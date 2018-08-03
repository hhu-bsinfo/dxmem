package de.hhu.bsinfo.dxmem.benchmark.operation;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkBenchmark;
import de.hhu.bsinfo.dxmem.data.ChunkState;

public class Get extends AbstractOperation {
    private final int m_maxChunkSize;
    private final ChunkBenchmark[] m_chunks;

    public Get(final float p_probability, final int p_batchCount, final boolean p_verifyData,
            final int p_maxChunkSize) {
        super("get", p_probability, p_batchCount, p_verifyData);

        m_maxChunkSize = p_maxChunkSize;

        m_chunks = new ChunkBenchmark[1024];
    }

    @Override
    public ChunkState execute(final DXMem p_memory, final boolean p_verifyData) {
        long cid = executeGetRandomCid();

        int tid = (int) Thread.currentThread().getId();

        if (m_chunks[tid] == null) {
            m_chunks[tid] = new ChunkBenchmark(m_maxChunkSize);
        }

        m_chunks[tid].setID(cid);

        executeTimeStart();
        p_memory.get().get(m_chunks[tid]);
        executeTimeEnd();

        // verify data in chunk
        if (p_verifyData && m_chunks[tid].isStateOk()) {
            if (!m_chunks[tid].verifyContents()) {
                return ChunkState.DATA_LOST;
            }
        }

        return m_chunks[tid].getState();
    }
}
