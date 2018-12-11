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
    public void create(final long[] p_cids, final int[] p_sizes) {
        CliContext.getInstance().getMemory().create().create(p_cids, 0, false, p_sizes);
    }

    @Override
    public void dump(final String p_outFile) {
        CliContext.getInstance().getMemory().dump().dump(p_outFile);
    }

    @Override
    public void get(final AbstractChunk[] p_chunks) {
        for (AbstractChunk chunk : p_chunks) {
            CliContext.getInstance().getMemory().get().get(chunk);
        }
    }

    @Override
    public void put(final AbstractChunk[] p_chunks) {
        for (AbstractChunk chunk : p_chunks) {
            CliContext.getInstance().getMemory().put().put(chunk);
        }
    }

    @Override
    public void remove(final AbstractChunk[] p_chunks) {
        for (AbstractChunk chunk : p_chunks) {
            CliContext.getInstance().getMemory().remove().remove(chunk);
        }
    }
}
