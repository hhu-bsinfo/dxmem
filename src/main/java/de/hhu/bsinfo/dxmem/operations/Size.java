package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;

public class Size {
    private final Context m_context;

    public Size(final Context p_context) {
        m_context = p_context;
    }

    public int size(final long p_cid) {
        CIDTableChunkEntry entry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, entry);

        if (!entry.isValid()) {
            return -1;
        }

        int size = m_context.getHeap().getSize(entry);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return size;
    }
}
