package de.hhu.bsinfo.dxmem.benchmark.operation;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkState;

public class Dump extends AbstractOperation {
    private final String m_outputFile;

    public Dump(final String p_outputFile) {
        super("dump", 1.0f, 1, false);

        m_outputFile = p_outputFile;
    }

    @Override
    protected ChunkState execute(final DXMem p_memory, final boolean p_verifyData) {
        executeTimeStart();
        p_memory.dump().dump(m_outputFile);
        executeTimeEnd();

        return ChunkState.OK;
    }
}
