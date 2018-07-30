package de.hhu.bsinfo.dxmem.benchmark.operation;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.RandomUtils;

public class Remove extends AbstractOperation {
    private final long m_cidRangeStart;
    private final long m_cidRangeEnd;
    private final ChunkByteArray m_chunk;

    public Remove(final float p_probability, final int p_batchCount, final boolean p_verifyData,
            final long p_cidRangeStart, final long p_cidRangeEnd) {
        super("create", p_probability, p_batchCount, p_verifyData);

        m_cidRangeStart = p_cidRangeStart;
        m_cidRangeEnd = p_cidRangeEnd;

        // dummy
        m_chunk = new ChunkByteArray(ChunkID.INVALID_ID, 1);
    }

    @Override
    public ChunkState execute(final DXMem p_memory, final boolean p_verifyData) {
        long cid = RandomUtils.getRandomValue(m_cidRangeStart, m_cidRangeEnd);

        m_chunk.setID(cid);

        p_memory.remove().remove(m_chunk);

        return m_chunk.getState();
    }
}
