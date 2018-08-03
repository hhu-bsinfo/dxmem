package de.hhu.bsinfo.dxmem.benchmark.operation;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkBenchmark;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.RandomUtils;

public class Create extends AbstractOperation {
    private final int m_minSize;
    private final int m_maxSize;

    private final ChunkBenchmark[] m_chunks;

    public Create(final float p_probability, final int p_batchCount, final boolean p_verifyData, final int p_minSize,
            final int p_maxSize) {
        super("create", p_probability, p_batchCount, p_verifyData);

        m_minSize = p_minSize;
        m_maxSize = p_maxSize;

        m_chunks = new ChunkBenchmark[1024];
    }
    
    @Override
    public ChunkState execute(final DXMem p_memory, final boolean p_verifyData) {
        int size;

        if (m_minSize == m_maxSize) {
            size = m_minSize;
        } else {
            size = RandomUtils.getRandomValue(m_minSize, m_maxSize);
        }

        // throw allocation exceptions. the benchmark cannot continue once that happens
        executeTimeStart();
        long cid = p_memory.create().create(size);
        executeTimeEnd();

        executeNewCid(cid);

        if (p_verifyData) {
            int tid = (int) Thread.currentThread().getId();

            if (m_chunks[tid] == null) {
                m_chunks[tid] = new ChunkBenchmark(m_maxSize);
            }

            m_chunks[tid].setID(cid);
            m_chunks[tid].setCurrentSize(size);
            m_chunks[tid].fillContents();

            p_memory.put().put(m_chunks[tid]);

            if (!m_chunks[tid].isStateOk()) {
                return ChunkState.UNDEFINED;
            }
        }

        return cid != ChunkID.INVALID_ID ? ChunkState.OK : ChunkState.UNDEFINED;
    }

    @Override
    public String parameterToString() {
        StringBuilder builder = new StringBuilder();

        builder.append(super.parameterToString());
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(',');
        builder.append("minSize,");
        builder.append(m_minSize);
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(',');
        builder.append("maxSize,");
        builder.append(m_maxSize);

        return builder.toString();
    }
}
