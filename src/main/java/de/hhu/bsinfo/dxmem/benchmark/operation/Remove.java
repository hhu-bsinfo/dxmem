package de.hhu.bsinfo.dxmem.benchmark.operation;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;

public class Remove extends AbstractOperation {
    private final ChunkByteArray m_chunk;

    public Remove(final float p_probability, final int p_batchCount, final boolean p_verifyData) {
        super("create", p_probability, p_batchCount, p_verifyData);

        // dummy
        m_chunk = new ChunkByteArray(ChunkID.INVALID_ID, 1);
    }

    @Override
    public ChunkState execute(final DXMem p_memory, final boolean p_verifyData) {
        long cid = executeGetRandomCid();

        // no chunks available, yet?
        if (cid == ChunkID.INVALID_ID) {
            return ChunkState.DOES_NOT_EXIST;
        }

        m_chunk.setID(cid);

        executeTimeStart();
        p_memory.remove().remove(m_chunk);
        executeTimeEnd();

        executeRemoveCid(cid);

        return m_chunk.getState();
    }
}
