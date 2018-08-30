package de.hhu.bsinfo.dxmem.operations;

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

    public ChunkIDRanges getCIDRangesOfLocalChunks() {
        return getCIDRangesOfChunks(m_context.getNodeId());
    }

    public ChunkIDRanges getCIDRangesOfChunks() {
        m_context.getDefragmenter().acquireApplicationThreadLock();

        ChunkIDRanges ranges = m_context.getCIDTable().getCIDRangesOfAllChunks();

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return ranges;
    }

    public ChunkIDRanges getCIDRangesOfChunks(final short p_nodeId) {
        m_context.getDefragmenter().acquireApplicationThreadLock();

        ChunkIDRanges ranges = m_context.getCIDTable().getCIDRangesOfAllChunks(p_nodeId);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return ranges;
    }

    public ChunkIDRanges getAllMigratedChunkIDRanges() {
        m_context.getDefragmenter().acquireApplicationThreadLock();

        ChunkIDRanges ranges = m_context.getCIDTable().getCIDRangesOfAllMigratedChunks();

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return ranges;
    }
}
