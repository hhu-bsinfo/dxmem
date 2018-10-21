package de.hhu.bsinfo.dxmem.benchmark;

import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.core.CIDTableStatus;
import de.hhu.bsinfo.dxmem.core.HeapStatus;
import de.hhu.bsinfo.dxmem.core.LIDStoreStatus;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;

/**
 * Implementation of BenchmarkContext interface for local DXMem benchmarks
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.09.2018
 */
public class DXMemBenchmarkContext implements BenchmarkContext {
    @Override
    public HeapStatus getHeapStatus() {
        return CliContext.getInstance().getMemory().stats().getHeapStatus();
    }

    @Override
    public CIDTableStatus getCIDTableStatus() {
        return CliContext.getInstance().getMemory().stats().getCIDTableStatus();
    }

    @Override
    public LIDStoreStatus getLIDStoreStatus() {
        return CliContext.getInstance().getMemory().stats().getLIDStoreStatus();
    }

    @Override
    public long create(final int p_size) {
        return CliContext.getInstance().getMemory().create().create(p_size);
    }

    @Override
    public void dump(final String p_outFile) {
        CliContext.getInstance().getMemory().dump().dump(p_outFile);
    }

    @Override
    public void get(final AbstractChunk p_chunk) {
        CliContext.getInstance().getMemory().get().get(p_chunk);
    }

    @Override
    public void put(final AbstractChunk p_chunk) {
        CliContext.getInstance().getMemory().put().put(p_chunk, ChunkLockOperation.ACQUIRE_OP_RELEASE, -1);
    }

    @Override
    public void remove(final AbstractChunk p_chunk) {
        CliContext.getInstance().getMemory().remove().remove(p_chunk);
    }
}
