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
import de.hhu.bsinfo.dxmem.core.LockManager;
import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Resize an existing chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public class Resize {
    private static final ValuePool SOP_RESIZE = new ValuePool(DXMem.class, "Resize");

    static {
        StatisticsManager.get().registerOperation(DXMem.class, SOP_RESIZE);
    }

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public Resize(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Resize an existing chunk
     *
     * @param p_cid
     *         CID of chunk to resize
     * @param p_newSize
     *         New size for chunk
     * @return True if succcessful, false on failure
     */
    public ChunkState resize(final long p_cid, final int p_newSize) {
        assert p_newSize > 0;

        if (m_context.isChunkLockDisabled()) {
            throw new MemoryRuntimeException("Not supporting resize operation if chunk locks are disabled");
        }

        if (p_cid == ChunkID.INVALID_ID) {
            return ChunkState.INVALID_ID;
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            return ChunkState.DOES_NOT_EXIST;
        }

        LockManager.LockStatus lockStatus = LockManager.executeBeforeOp(m_context.getCIDTable(), tableEntry,
                ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, -1);

        // use write lock because we might have to change the address and modify metadata
        if (lockStatus != LockManager.LockStatus.OK) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            // someone else deleted the chunk while waiting for the lock
            return ChunkState.DOES_NOT_EXIST;
        }

        m_context.getHeap().resize(tableEntry, p_newSize);

        // update cid table entry
        m_context.getCIDTable().entryUpdate(tableEntry);

        LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry, ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_RESIZE.inc();

        return ChunkState.OK;
    }
}
