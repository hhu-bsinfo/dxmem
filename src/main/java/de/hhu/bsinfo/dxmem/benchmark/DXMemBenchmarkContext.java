package de.hhu.bsinfo.dxmem.benchmark;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.core.CIDTableStatus;
import de.hhu.bsinfo.dxmem.core.HeapStatus;
import de.hhu.bsinfo.dxmem.core.LIDStoreStatus;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;

/**
 * Implementation of BenchmarkContext interface for local DXMem benchmarks
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.09.2018
 */
public class DXMemBenchmarkContext implements BenchmarkContext {
    private final DXMem m_mem;

    /**
     * Constructor
     *
     * @param p_mem
     *         Local DXMem instance
     */
    public DXMemBenchmarkContext(final DXMem p_mem) {
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
    public long create(final int p_size) {
        return m_mem.create().create(p_size);
    }

    @Override
    public void dump(final String p_outFile) {
        m_mem.dump().dump(p_outFile);
    }

    @Override
    public void get(final AbstractChunk p_chunk) {
        m_mem.get().get(p_chunk);
    }

    @Override
    public void put(final AbstractChunk p_chunk) {
        m_mem.put().put(p_chunk);
    }

    @Override
    public void remove(final AbstractChunk p_chunk) {
        m_mem.remove().remove(p_chunk);
    }
}
