package de.hhu.bsinfo.dxmem.cli;

import de.hhu.bsinfo.dxmem.DXMem;

public class CliContext {
    private final static CliContext ms_instance = new CliContext();

    private DXMem m_memory;

    // singletons are bad but having to use a dependency injection framework
    // for a single object is overkill
    public static CliContext getInstance() {
        return ms_instance;
    }

    private CliContext() {

    }

    public boolean isMemoryLoaded() {
        return m_memory != null;
    }

    public void newMemory(final short p_nodeId, final long p_heapSize) {
        if (m_memory != null) {
            m_memory.shutdown();
        }

        m_memory = new DXMem(p_nodeId, p_heapSize);
    }

    public void loadFromFile(final String p_inFile) {
        if (m_memory != null) {
            m_memory.shutdown();
        }

        m_memory = new DXMem(p_inFile);
    }

    public DXMem getMemory() {
        return m_memory;
    }
}
