package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;

public class Exists {
    private final Context m_context;

    public Exists(final Context p_context) {
        m_context = p_context;
    }

    public boolean exists(final long p_cid) {
        CIDTableChunkEntry entry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, entry);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return entry.isValid();
    }
}
