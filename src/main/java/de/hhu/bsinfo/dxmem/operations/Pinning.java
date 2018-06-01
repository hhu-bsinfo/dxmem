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

import de.hhu.bsinfo.dxmem.core.Address;
import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.LockUtils;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;

public class Pinning {
    private final Context m_context;

    public Pinning(final Context p_context) {
        m_context = p_context;
    }

    public long pin(final ChunkState p_state, final long p_cid, final int p_acquireLockTimeoutMs) {
        if (p_cid == ChunkID.INVALID_ID) {
            // TODO how to return chunk state to give more insight on what happened here
            //p_state = ChunkState.INVALID_ID;
            return Address.INVALID;
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            return Address.INVALID;
        }

        LockUtils.LockStatus lockStatus = LockUtils.acquireWriteLock(m_context.getCIDTable(), tableEntry,
                p_acquireLockTimeoutMs);

        // acquire write lock to ensure the chunk is not deleted while trying to pin it
        if (lockStatus != LockUtils.LockStatus.OK) {
            // TODO return chunk state to give more insight on what happened here
            m_context.getDefragmenter().releaseApplicationThreadLock();

            return Address.INVALID;
        }

        tableEntry.setPinned(true);

        m_context.getCIDTable().entryUpdate(tableEntry);

        LockUtils.releaseWriteLock(m_context.getCIDTable(), tableEntry);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return tableEntry.getAddress();
    }

    // depending on how many chunks are currently stored, this call is very slow because it has to perform
    // a depth search on the CIDTable to find the CIDTable entry
    public long unpin(final long p_pinnedChunkAddress) {
        if (p_pinnedChunkAddress == Address.INVALID || p_pinnedChunkAddress < 0) {
            return ChunkID.INVALID_ID;
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        long cid = m_context.getCIDTable().getTableEntryWithChunkAddress(tableEntry, p_pinnedChunkAddress);

        if (!tableEntry.isValid()) {
            throw new IllegalStateException("Could not find chunk entry in CIDTable for raw chunk address " +
                    Address.toHexString(p_pinnedChunkAddress) + " ensure the address is valid");
        }

        if (!tableEntry.isPinned()) {
            throw new IllegalStateException("Cannot unpin chunk with CID " + ChunkID.toHexString(cid) +
                    " with chunk table entry " + tableEntry + ", not previously pinned");
        }

        LockUtils.LockStatus lockStatus = LockUtils.acquireWriteLock(m_context.getCIDTable(), tableEntry, -1);

        // acquire write lock to ensure the chunk is not deleted while trying to pin it
        if (lockStatus != LockUtils.LockStatus.OK) {
            // TODO return chunk state to give more insight on what happened here
            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new IllegalStateException("Pinned chunk with CID " + ChunkID.toHexString(cid) +
                    " with chunk table entry " + tableEntry + " deleted while waiting for write lock");
        }

        tableEntry.setPinned(false);

        m_context.getCIDTable().entryUpdate(tableEntry);

        LockUtils.releaseWriteLock(m_context.getCIDTable(), tableEntry);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return cid;
    }

    public void unpinCID(final long p_cidOfPinnedChunk) {
        if (p_cidOfPinnedChunk == ChunkID.INVALID_ID) {
            return;
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cidOfPinnedChunk, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new IllegalStateException("Table entry for CID " + ChunkID.toHexString(p_cidOfPinnedChunk) +
                    " is supposed to be pinned (?) but entry is not valid");
        }

        LockUtils.LockStatus lockStatus = LockUtils.acquireWriteLock(m_context.getCIDTable(), tableEntry, -1);

        // acquire write lock to ensure the chunk is not deleted while trying to pin it
        if (lockStatus != LockUtils.LockStatus.OK) {
            // TODO return chunk state to give insight on what happened here
            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new IllegalStateException("Pinned chunk " + ChunkID.toHexString(p_cidOfPinnedChunk) +
                    " deleted while waiting for write lock");
        }

        tableEntry.setPinned(false);

        m_context.getCIDTable().entryUpdate(tableEntry);

        LockUtils.releaseWriteLock(m_context.getCIDTable(), tableEntry);

        m_context.getDefragmenter().releaseApplicationThreadLock();
    }
}
