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

import de.hhu.bsinfo.dxmem.DXMemory;
import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.HeapDataStructureImExporter;
import de.hhu.bsinfo.dxmem.core.LockUtils;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * Put modified data of a chunk back to the heap
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public class Put {
    private static final Value SOP_PUT = new Value(DXMemory.class, "Put");
    private static final Value SOP_PUT_INVALID_ID = new Value(DXMemory.class, "PutInvalidID");
    private static final Value SOP_PUT_NOT_EXISTS = new Value(DXMemory.class, "PutNotExists");

    static {
        StatisticsManager.get().registerOperation(DXMemory.class, SOP_PUT);
        StatisticsManager.get().registerOperation(DXMemory.class, SOP_PUT_INVALID_ID);
        StatisticsManager.get().registerOperation(DXMemory.class, SOP_PUT_NOT_EXISTS);
    }

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context with core components
     */
    public Put(final Context p_context) {
        m_context = p_context;
    }

    // used with local puts and known data structure (type)
    // @return True on success, false on failure. Chunk state with additional information is set in p_chunk
    public boolean put(final AbstractChunk p_chunk, final ChunkLockOperation p_lockOperation,
            final int p_lockTimeoutMs) {
        if (p_chunk.getID() == ChunkID.INVALID_ID) {
            p_chunk.setState(ChunkState.INVALID_ID);
            SOP_PUT_INVALID_ID.inc();
            return false;
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_chunk.getID(), tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            p_chunk.setState(ChunkState.DOES_NOT_EXIST);
            SOP_PUT_NOT_EXISTS.inc();
            return false;
        }

        HeapDataStructureImExporter imExporter = m_context.getDataStructureImExporterPool().get();
        imExporter.setHeapAddress(tableEntry.getAddress());

        LockUtils.LockStatus lockStatus = LockUtils.LockStatus.OK;

        if (p_lockOperation == ChunkLockOperation.NONE) {
            lockStatus = LockUtils.acquireReadLock(m_context.getCIDTable(), tableEntry, p_lockTimeoutMs);
        } else {
            if (p_lockOperation == ChunkLockOperation.ACQUIRE_BEFORE_OP ||
                    p_lockOperation == ChunkLockOperation.ACQUIRE_OP_RELEASE) {
                lockStatus = LockUtils.acquireWriteLock(m_context.getCIDTable(), tableEntry, p_lockTimeoutMs);
            }
        }

        if (lockStatus != LockUtils.LockStatus.OK) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            if (lockStatus == LockUtils.LockStatus.INVALID) {
                // entry was deleted in the meanwhile
                p_chunk.setState(ChunkState.DOES_NOT_EXIST);
            } else if (lockStatus == LockUtils.LockStatus.TIMEOUT) {
                // try lock did not succeed
                p_chunk.setState(ChunkState.LOCK_TIMEOUT);
            } else {
                throw new IllegalStateException();
            }

            return false;
        }

        // TODO wrap with try catch and catch memory runtime exception? are there any left to catch?
        // -> memory dump on error

        imExporter.exportObject(p_chunk);

        if (p_lockOperation == ChunkLockOperation.NONE) {
            LockUtils.releaseReadLock(m_context.getCIDTable(), tableEntry);
        } else {
            if (p_lockOperation == ChunkLockOperation.RELEASE_AFTER_OP ||
                    p_lockOperation == ChunkLockOperation.ACQUIRE_OP_RELEASE) {
                LockUtils.releaseWriteLock(m_context.getCIDTable(), tableEntry);
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        p_chunk.setState(ChunkState.OK);

        SOP_PUT.inc();

        return true;
    }

    // used for incoming remote requests with binary data blob only (no type information)
    public ChunkState put(final long p_chunkID, final byte[] p_data, final ChunkLockOperation p_lockOperation,
            final int p_lockTimeoutMs) {
        if (p_chunkID == ChunkID.INVALID_ID) {
            SOP_PUT_INVALID_ID.inc();
            return ChunkState.INVALID_ID;
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_chunkID, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            SOP_PUT_NOT_EXISTS.inc();
            return ChunkState.DOES_NOT_EXIST;
        }

        HeapDataStructureImExporter imExporter = m_context.getDataStructureImExporterPool().get();
        imExporter.setHeapAddress(tableEntry.getAddress());

        LockUtils.LockStatus lockStatus = LockUtils.LockStatus.OK;

        if (p_lockOperation == ChunkLockOperation.NONE) {
            lockStatus = LockUtils.acquireReadLock(m_context.getCIDTable(), tableEntry, p_lockTimeoutMs);
        } else {
            if (p_lockOperation == ChunkLockOperation.ACQUIRE_BEFORE_OP ||
                    p_lockOperation == ChunkLockOperation.ACQUIRE_OP_RELEASE) {
                lockStatus = LockUtils.acquireWriteLock(m_context.getCIDTable(), tableEntry, p_lockTimeoutMs);
            }
        }

        if (lockStatus != LockUtils.LockStatus.OK) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            if (lockStatus == LockUtils.LockStatus.INVALID) {
                // entry was deleted in the meanwhile
                return ChunkState.DOES_NOT_EXIST;
            } else if (lockStatus == LockUtils.LockStatus.TIMEOUT) {
                // try lock did not succeed
                return ChunkState.LOCK_TIMEOUT;
            } else {
                throw new IllegalStateException();
            }
        }

        // TODO wrap with try catch and catch memory runtime exception? are there any left to catch?
        // -> memory dump on error

        // because this is quite expensive, keep this as an assert to catch writing beyond the chunk bounds
        assert p_data.length <= m_context.getHeap().getSize(tableEntry);

        imExporter.writeBytes(p_data);

        if (p_lockOperation == ChunkLockOperation.NONE) {
            LockUtils.releaseReadLock(m_context.getCIDTable(), tableEntry);
        } else {
            if (p_lockOperation == ChunkLockOperation.RELEASE_AFTER_OP ||
                    p_lockOperation == ChunkLockOperation.ACQUIRE_OP_RELEASE) {
                LockUtils.releaseWriteLock(m_context.getCIDTable(), tableEntry);
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_PUT.inc();

        return ChunkState.OK;
    }
}
