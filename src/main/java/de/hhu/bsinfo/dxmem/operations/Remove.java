/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.LockUtils;
import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * Remove chunks
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public class Remove {
    private static final Value SOP_REMOVE = new Value(DXMem.class, "Remove");
    private static final Value SOP_REMOVE_MIGRATED = new Value(DXMem.class, "RemoveMigrated");

    static {
        StatisticsManager.get().registerOperation(DXMem.class, SOP_REMOVE);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_REMOVE_MIGRATED);
    }

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public Remove(final Context p_context) {
        m_context = p_context;
    }

    /**
     * For migrated chunks: if deleted on remote, the remote has to send a message to the original owner to tell it
     * that the cid is not used anymore and can be re-used
     *
     * @param p_cid
     *         CID to reuse
     */
    public void prepareChunkIDForReuse(final long p_cid) {
        if (!m_context.isChunkLockDisabled()) {
            throw new MemoryRuntimeException("Not supporting remove operation if chunk locks are disabled");
        }

        // sanity check
        if (ChunkID.getCreatorID(p_cid) != m_context.getNodeId()) {
            throw new IllegalStateException("Cannot reuse foreign cid " + ChunkID.toHexString(p_cid) + " on node " +
                    NodeID.toHexString(m_context.getNodeId()));
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (tableEntry.isValid()) {
            throw new IllegalStateException("Table value " + tableEntry + " is valid for cid " +
                    ChunkID.toHexString(p_cid));
        }

        if (!m_context.getLIDStore().put(ChunkID.getLocalID(p_cid))) {
            // LID store full, store as zombie
            m_context.getCIDTable().entryFlagZombie(tableEntry);
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();
    }

    /**
     * Remove a chunk
     *
     * @param p_ds
     *         Chunk to remove
     * @return On success, size of chunk removed, on failure negative ChunkState
     */
    public int remove(final AbstractChunk p_ds) {
        int state = remove(p_ds.getID(), false);

        if (state < 0) {
            p_ds.setState(ChunkState.values()[-state]);
        }

        return state;
    }

    /**
     * Remove a chunk
     *
     * @param p_ds
     *         Chunk to remove
     * @param p_wasMigrated
     *         True if the chunk was migrated, false otherwise
     * @return On success, size of chunk removed, on failure negative ChunkState
     */
    public int remove(final AbstractChunk p_ds, final boolean p_wasMigrated) {
        int state = remove(p_ds.getID(), p_wasMigrated);

        if (state < 0) {
            p_ds.setState(ChunkState.values()[-state]);
        }

        return state;
    }

    /**
     * Remove a chunk
     *
     * @param p_cid
     *         CID of chunk to remove
     * @return On success, size of chunk removed, on failure negative ChunkState
     */
    public int remove(final long p_cid) {
        return remove(p_cid, false);
    }

    /**
     * Remove a chunk
     *
     * @param p_cid
     *         CID of chunk to remove
     * @param p_wasMigrated
     *         True if the chunk was migrated, false otherwise
     * @return On success, size of chunk removed, on failure negative ChunkState
     */
    public int remove(final long p_cid, final boolean p_wasMigrated) {
        if (!m_context.isChunkLockDisabled()) {
            throw new MemoryRuntimeException("Not supporting remove operation if chunk locks are disabled");
        }

        if (p_cid == ChunkID.INVALID_ID) {
            return -ChunkState.INVALID_ID.ordinal();
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            // already deleted or never existed
            return -ChunkState.DOES_NOT_EXIST.ordinal();
        }

        if (tableEntry.isPinned()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new MemoryRuntimeException("Cannot remove pinned chunk " + ChunkID.toHexString(p_cid));
        }

        // acquire write lock to ensure nobody is accessing the chunk while deleting it
        if (LockUtils.acquireWriteLock(m_context.getCIDTable(), tableEntry, -1) != LockUtils.LockStatus.OK) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            // someone else deleted the chunk while waiting for the lock
            return -ChunkState.DOES_NOT_EXIST.ordinal();
        }

        int chunkSize = m_context.getHeap().getSize(tableEntry);

        // no need to unlock the entry because flagging it free will kill it anyway
        m_context.getCIDTable().entryFlagFree(tableEntry);

        // only lids of non migrated chunks go back into the lid store
        if (!p_wasMigrated && !m_context.getLIDStore().put(p_cid)) {
            // lid store full, flag as zombie
            m_context.getCIDTable().entryFlagZombie(tableEntry);
        }

        // at last, free chunk memory
        m_context.getHeap().free(tableEntry);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        if (p_wasMigrated) {
            SOP_REMOVE_MIGRATED.inc();
        } else {
            SOP_REMOVE.inc();
        }

        return chunkSize;
    }
}
