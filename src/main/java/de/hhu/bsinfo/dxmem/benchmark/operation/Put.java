package de.hhu.bsinfo.dxmem.benchmark.operation;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.RandomUtils;

public class Put extends AbstractOperation {
    private final long m_cidRangeStart;
    private final long m_cidRangeEnd;
    private final ChunkByteArray m_chunk;

    public Put(final float p_probability, final int p_batchCount, final boolean p_verifyData,
            final long p_cidRangeStart, final long p_cidRangeEnd, final int p_chunkSize) {
        super("put", p_probability, p_batchCount, p_verifyData);

        m_cidRangeStart = p_cidRangeStart;
        m_cidRangeEnd = p_cidRangeEnd;
        m_chunk = new ChunkByteArray(ChunkID.INVALID_ID, p_chunkSize);
    }

    @Override
    public ChunkState execute(final DXMem p_memory, final boolean p_verifyData) {
        long cid = RandomUtils.getRandomValue(m_cidRangeStart, m_cidRangeEnd);

        m_chunk.setID(cid);

        if (p_verifyData) {
            // write data to chunk for verification
            for (int i = 0; i < m_chunk.getData().length; i++) {
                m_chunk.getData()[i] = (byte) i;
            }
        }

        p_memory.put().put(m_chunk);

        return m_chunk.getState();
    }
}
