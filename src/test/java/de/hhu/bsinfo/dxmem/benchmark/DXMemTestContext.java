package de.hhu.bsinfo.dxmem.benchmark;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.core.CIDTableStatus;
import de.hhu.bsinfo.dxmem.core.HeapStatus;
import de.hhu.bsinfo.dxmem.core.LIDStoreStatus;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;

public class DXMemTestContext implements BenchmarkContext {
    private final DXMem m_mem;

    public DXMemTestContext(final DXMem p_mem) {
        m_mem = p_mem;
    }

    @Override
    public HeapStatus getHeapStatus() {
        return m_mem.stats().getHeapStatus();
    }

    @Override
    public CIDTableStatus getCIDTableStatus() {
        return m_mem.stats().getCIDTableStatus();
    }

    @Override
    public LIDStoreStatus getLIDStoreStatus() {
        return m_mem.stats().getLIDStoreStatus();
    }

    @Override
    public void create(final long[] p_cids, final int[] p_sizes) {
        for (int i = 0; i < p_sizes.length; i++) {
            p_cids[i] = m_mem.create().create(p_sizes[i]);
        }
    }

    @Override
    public void dump(final String p_outFile) {
        m_mem.dump().dump(p_outFile);
    }

    @Override
    public void get(final AbstractChunk[] p_chunks) {
        for (AbstractChunk chunk : p_chunks) {
            m_mem.get().get(chunk);
        }
    }

    @Override
    public void put(final AbstractChunk[] p_chunks) {
        for (AbstractChunk chunk : p_chunks) {
            m_mem.put().put(chunk);
        }
    }

    @Override
    public void remove(final AbstractChunk[] p_chunks) {
        for (AbstractChunk chunk : p_chunks) {
            m_mem.remove().remove(chunk);
        }
    }
}
