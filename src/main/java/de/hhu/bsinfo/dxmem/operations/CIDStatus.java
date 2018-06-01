package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;

public class CIDStatus {
    private final Context m_context;

    public CIDStatus(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Returns the highest LocalID currently in use
     *
     * @return the LocalID
     */
    public long getHighestUsedLocalID() {
        return m_context.getLIDStore().getCurrentHighestLID();
    }

    public boolean exists(final long p_cid) {
        CIDTableChunkEntry entry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, entry);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return entry.isValid();
    }

    /**
     * Returns the ChunkID ranges of all locally stored Chunks
     *
     * @return the ChunkID ranges
     */
    public ChunkIDRanges getCIDRangesOfAllLocalChunks() {
        return m_context.getCIDTable().getCIDRangesOfAllChunks(m_context.getNodeId());
    }

    public ChunkIDRanges getAllMigratedChunkIDRanges() {
        return m_context.getCIDTable().getCIDRangesOfAllChunks();
    }
}
