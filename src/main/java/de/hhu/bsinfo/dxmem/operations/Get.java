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
import de.hhu.bsinfo.dxmem.core.HeapDataStructureImExporter;
import de.hhu.bsinfo.dxmem.core.LockUtils;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * Get the data of a chunk from the heap
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public final class Get {
    private static final Value SOP_GET = new Value(DXMem.class, "Get");
    private static final Value SOP_GET_INVALID_ID = new Value(DXMem.class, "GetInvalidID");
    private static final Value SOP_GET_NOT_EXISTS = new Value(DXMem.class, "GetNotExists");

    static {
        StatisticsManager.get().registerOperation(DXMem.class, SOP_GET);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_GET_INVALID_ID);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_GET_NOT_EXISTS);
    }

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public Get(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Get a single chunk
     *
     * @param p_chunk
     *         AbstractChunk with the CID set to read the chunk's data into. On success, the payload is
     *         serialized into the AbstractChunk and the state is set to ok. On failure, the AbstractChunk
     *         contains no (new) data or partial serialized data and the state indicates the failure.
     * @return True if successful, false on error. Chunk state with additional information is set in p_chunk
     */
    public boolean get(final AbstractChunk p_chunk) {
        return get(p_chunk, ChunkLockOperation.NONE, -1);
    }

    /**
     * Get the data of a chunk from the heap memory. Used for local gets.
     *
     * @param p_chunk
     *         AbstractChunk with the CID set to read the chunk's data into. On success, the payload is
     *         serialized into the AbstractChunk and the state is set to ok. On failure, the AbstractChunk
     *         contains no (new) data or partial serialized data and the state indicates the failure.
     * @param p_lockOperation
     *         Lock operation to execute with this get operation on the chunk
     * @param p_lockTimeoutMs
     *         If a lock operation is set, set to -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and > 0 for a timeout value in ms
     * @return True on success, false on failure. Chunk state with additional information is set in p_chunk
     */
    public boolean get(final AbstractChunk p_chunk, final ChunkLockOperation p_lockOperation,
            final int p_lockTimeoutMs) {
        if (p_chunk.getID() == ChunkID.INVALID_ID) {
            p_chunk.setState(ChunkState.INVALID_ID);
            SOP_GET_INVALID_ID.inc();
            return false;
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_chunk.getID(), tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            p_chunk.setState(ChunkState.DOES_NOT_EXIST);
            SOP_GET_NOT_EXISTS.inc();
            return false;
        }

        HeapDataStructureImExporter imExporter = m_context.getDataStructureImExporterPool().get();
        imExporter.setHeapAddress(tableEntry.getAddress());

        LockUtils.LockStatus lockStatus = LockUtils.LockStatus.OK;

        if (!m_context.isChunkLockDisabled()) {
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
        }

        // TODO wrap with try catch and catch memory runtime exception? are there any left to catch?
        // -> memory dump on error

        imExporter.importObject(p_chunk);

        if (!m_context.isChunkLockDisabled()) {
            if (p_lockOperation == ChunkLockOperation.NONE) {
                LockUtils.releaseReadLock(m_context.getCIDTable(), tableEntry);
            } else {
                if (p_lockOperation == ChunkLockOperation.RELEASE_AFTER_OP ||
                        p_lockOperation == ChunkLockOperation.ACQUIRE_OP_RELEASE) {
                    LockUtils.releaseWriteLock(m_context.getCIDTable(), tableEntry);
                }
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        p_chunk.setState(ChunkState.OK);

        SOP_GET.inc();

        return true;
    }

    /**
     * Get the data of a chunk from the heap memory. Used in incoming gets when the type is unknown and we just want
     * to grab the binary data to forward to the requester
     *
     * @param p_cid
     *         CID of the chunk to get
     * @return ChunkByteArray. Chunk state determines success or failure of the operation
     */
    public ChunkByteArray get(final long p_cid) {
        return get(p_cid, ChunkLockOperation.NONE, -1);
    }

    /**
     * Get the data of a chunk from the heap memory. Used in incoming gets when the type is unknown and we just want
     * to grab the binary data to forward to the requester
     *
     * @param p_cid
     *         CID of the chunk to get
     * @param p_lockOperation
     *         Lock operation to execute with this get operation on the chunk
     * @param p_lockTimeoutMs
     *         If a lock operation is set, set to -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and > 0 for a timeout value in ms
     * @return ChunkByteArray. Chunk state determines success or failure of the operation
     */
    public ChunkByteArray get(final long p_cid, final ChunkLockOperation p_lockOperation,
            final int p_lockTimeoutMs) {
        if (p_cid == ChunkID.INVALID_ID) {
            SOP_GET_INVALID_ID.inc();

            ChunkByteArray ret = new ChunkByteArray(0);
            ret.setID(p_cid);
            ret.setState(ChunkState.INVALID_ID);
            return ret;
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            SOP_GET_NOT_EXISTS.inc();

            ChunkByteArray ret = new ChunkByteArray(0);
            ret.setID(p_cid);
            ret.setState(ChunkState.DOES_NOT_EXIST);
            return ret;
        }

        HeapDataStructureImExporter imExporter = m_context.getDataStructureImExporterPool().get();
        imExporter.setHeapAddress(tableEntry.getAddress());

        LockUtils.LockStatus lockStatus = LockUtils.LockStatus.OK;

        if (!m_context.isChunkLockDisabled()) {
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
                    ChunkByteArray ret = new ChunkByteArray(0);
                    ret.setID(p_cid);
                    ret.setState(ChunkState.DOES_NOT_EXIST);
                    return ret;
                } else if (lockStatus == LockUtils.LockStatus.TIMEOUT) {
                    // try lock did not succeed
                    ChunkByteArray ret = new ChunkByteArray(0);
                    ret.setID(p_cid);
                    ret.setState(ChunkState.LOCK_TIMEOUT);
                    return ret;
                } else {
                    throw new IllegalStateException();
                }
            }
        }

        // TODO wrap with try catch and catch memory runtime exception? are there any left to catch?
        // -> memory dump on error

        byte[] data = new byte[m_context.getHeap().getSize(tableEntry)];
        imExporter.readBytes(data);

        if (!m_context.isChunkLockDisabled()) {
            if (p_lockOperation == ChunkLockOperation.NONE) {
                LockUtils.releaseReadLock(m_context.getCIDTable(), tableEntry);
            } else {
                if (p_lockOperation == ChunkLockOperation.RELEASE_AFTER_OP ||
                        p_lockOperation == ChunkLockOperation.ACQUIRE_OP_RELEASE) {
                    LockUtils.releaseWriteLock(m_context.getCIDTable(), tableEntry);
                }
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        ChunkByteArray ret = new ChunkByteArray(data);
        ret.setID(p_cid);
        ret.setState(ChunkState.OK);

        SOP_GET.inc();

        return ret;
    }

    /**
     * Get the data of a chunk from the heap memory. Used for replicating chunks. returns 0 if data does not fit into
     * buffer (and on other errors) returns size on success
     *
     * @param p_cid
     *         CID of the chunk to get
     * @param p_buffer
     *         Pre-allocated buffer to write chunk payload to
     * @param p_offset
     *         Offset in buffer to start at
     * @param p_size
     *         Size of buffer
     * @param p_lockOperation
     *         Lock operation to execute with this get operation on the chunk
     * @param p_lockTimeoutMs
     *         If a lock operation is set, set to -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and > 0 for a timeout value in ms
     * @return ChunkByteArray. Chunk state determines success or failure of the operation
     */
    public int get(final long p_cid, final byte[] p_buffer, final int p_offset, final int p_size,
            final ChunkLockOperation p_lockOperation, final int p_lockTimeoutMs) {
        if (p_cid == ChunkID.INVALID_ID) {
            SOP_GET_INVALID_ID.inc();

            return -ChunkState.INVALID_ID.ordinal();
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            SOP_GET_NOT_EXISTS.inc();

            return -ChunkState.DOES_NOT_EXIST.ordinal();
        }

        int chunkSize = m_context.getHeap().getSize(tableEntry);

        // abort if buffer is not large enough
        if (chunkSize > p_size) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            return -ChunkState.UNDEFINED.ordinal();
        }

        HeapDataStructureImExporter imExporter = m_context.getDataStructureImExporterPool().get();
        imExporter.setHeapAddress(tableEntry.getAddress());

        LockUtils.LockStatus lockStatus = LockUtils.LockStatus.OK;

        if (!m_context.isChunkLockDisabled()) {
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
                    return -ChunkState.DOES_NOT_EXIST.ordinal();
                } else if (lockStatus == LockUtils.LockStatus.TIMEOUT) {
                    // try lock did not succeed
                    return -ChunkState.LOCK_TIMEOUT.ordinal();
                } else {
                    throw new IllegalStateException();
                }
            }
        }

        // TODO wrap with try catch and catch memory runtime exception? are there any left to catch?
        // -> memory dump on error

        imExporter.readBytes(p_buffer, p_offset, chunkSize);

        if (!m_context.isChunkLockDisabled()) {
            if (p_lockOperation == ChunkLockOperation.NONE) {
                LockUtils.releaseReadLock(m_context.getCIDTable(), tableEntry);
            } else {
                if (p_lockOperation == ChunkLockOperation.RELEASE_AFTER_OP ||
                        p_lockOperation == ChunkLockOperation.ACQUIRE_OP_RELEASE) {
                    LockUtils.releaseWriteLock(m_context.getCIDTable(), tableEntry);
                }
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_GET.inc();

        return chunkSize;
    }
}
