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

import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.LockUtils;
import de.hhu.bsinfo.dxmem.data.ChunkID;

public class Resize {
    private final Context m_context;

    public Resize(final Context p_context) {
        m_context = p_context;
    }

    public boolean resize(final long p_cid, final int p_newSize) {
        // TODO statistics
        assert p_newSize > 0;

        if (p_cid == ChunkID.INVALID_ID) {
            return false;
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            return false;
        }

        LockUtils.LockStatus lockStatus = LockUtils.acquireWriteLock(m_context.getCIDTable(), tableEntry, -1);

        // use write lock because we might have to change the address and modify metadata
        if (lockStatus != LockUtils.LockStatus.OK) {
            // TODO return chunk state to give more insight on what happened here
            m_context.getDefragmenter().releaseApplicationThreadLock();

            // someone else deleted the chunk while waiting for the lock
            return false;
        }

        m_context.getHeap().resize(tableEntry, p_newSize);

        LockUtils.releaseWriteLock(m_context.getCIDTable(), tableEntry);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return true;
    }
}
