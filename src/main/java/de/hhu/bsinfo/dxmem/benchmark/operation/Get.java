package de.hhu.bsinfo.dxmem.benchmark.operation;

import de.hhu.bsinfo.dxmem.DXMemory;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.RandomUtils;

public class Get extends AbstractOperation {
    private final long m_cidRangeStart;
    private final long m_cidRangeEnd;
    private final ChunkByteArray m_chunk;

    public Get(final float p_probability, final int p_batchCount, final long p_cidRangeStart,
            final long p_cidRangeEnd, final int p_chunkSize) {
        super("get", p_probability, p_batchCount);

        m_cidRangeStart = p_cidRangeStart;
        m_cidRangeEnd = p_cidRangeEnd;
        m_chunk = new ChunkByteArray(ChunkID.INVALID_ID, p_chunkSize);
    }

    @Override
    public ChunkState execute(final DXMemory p_memory) {
        long cid = RandomUtils.getRandomValue(m_cidRangeStart, m_cidRangeEnd);

        m_chunk.setID(cid);

        p_memory.get().get(m_chunk);

        return m_chunk.getState();
    }
}
