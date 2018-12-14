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
import de.hhu.bsinfo.dxmem.core.LockManager;
import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Put modified data of a chunk back to the heap
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public class Put {
    private static final ValuePool SOP_PUT = new ValuePool(DXMem.class, "Put");
    private static final ValuePool SOP_PUT_INVALID_ID = new ValuePool(DXMem.class, "PutInvalidID");
    private static final ValuePool SOP_PUT_NOT_EXISTS = new ValuePool(DXMem.class, "PutNotExists");

    static {
        StatisticsManager.get().registerOperation(DXMem.class, SOP_PUT);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_PUT_INVALID_ID);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_PUT_NOT_EXISTS);
    }

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public Put(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Get the data of a chunk from the heap memory. Used with local puts and known chunks.
     *
     * @param p_chunk
     *         AbstractChunk with the CID set and data to write to the heap. On success, the payload is
     *         serialized into the heap and the state is set to ok. On failure, the AbstractChunk
     *         state indicates the cause.
     * @return True on success, false on failure. Chunk state with additional information is set in p_chunk
     */
    public boolean put(final AbstractChunk p_chunk) {
        return put(p_chunk, ChunkLockOperation.WRITE_LOCK_ACQ_OP_REL, -1);
    }

    /**
     * Get the data of a chunk from the heap memory. Used with local puts and known chunks.
     *
     * @param p_chunk
     *         AbstractChunk with the CID set and data to write to the heap. On success, the payload is
     *         serialized into the heap and the state is set to ok. On failure, the AbstractChunk
     *         state indicates the cause.
     * @param p_lockOperation
     *         Lock operation to execute with this put operation on the chunk
     * @param p_lockTimeoutMs
     *         If a lock operation is set, set to -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and &gt; 0 for a timeout value in ms
     * @return True on success, false on failure. Chunk state with additional information is set in p_chunk
     */
    public boolean put(final AbstractChunk p_chunk, final ChunkLockOperation p_lockOperation,
            final int p_lockTimeoutMs) {
        assert assertLockOperationSupport(p_lockOperation);

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

        if (!m_context.isChunkLockDisabled()) {
            LockManager.LockStatus lockStatus = LockManager.executeBeforeOp(m_context.getCIDTable(), tableEntry,
                    p_lockOperation, p_lockTimeoutMs);

            if (lockStatus != LockManager.LockStatus.OK) {
                m_context.getDefragmenter().releaseApplicationThreadLock();

                if (lockStatus == LockManager.LockStatus.INVALID) {
                    // entry was deleted in the meanwhile
                    p_chunk.setState(ChunkState.DOES_NOT_EXIST);
                } else if (lockStatus == LockManager.LockStatus.TIMEOUT) {
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

        imExporter.exportObject(p_chunk);

        if (!m_context.isChunkLockDisabled()) {
            LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry, p_lockOperation, p_lockTimeoutMs);
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        p_chunk.setState(ChunkState.OK);

        SOP_PUT.inc();

        return true;
    }

    /**
     * Get the data of a chunk from the heap memory. Used for incoming remote requests with binary data blob only
     * (no type information)
     *
     * @param p_chunkID
     *         CID of the chunk to put
     * @param p_data
     *         Pre-allocated buffer with data write
     * @param p_lockOperation
     *         Lock operation to execute with this put operation on the chunk
     * @param p_lockTimeoutMs
     *         If a lock operation is set, set to -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and &gt; 0 for a timeout value in ms
     * @return Chunk state with results of operation
     */
    public ChunkState put(final long p_chunkID, final byte[] p_data, final ChunkLockOperation p_lockOperation,
            final int p_lockTimeoutMs) {
        assert assertLockOperationSupport(p_lockOperation);

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

        if (!m_context.isChunkLockDisabled()) {
            LockManager.LockStatus lockStatus = LockManager.executeBeforeOp(m_context.getCIDTable(), tableEntry,
                    p_lockOperation, p_lockTimeoutMs);

            if (lockStatus != LockManager.LockStatus.OK) {
                m_context.getDefragmenter().releaseApplicationThreadLock();

                if (lockStatus == LockManager.LockStatus.INVALID) {
                    // entry was deleted in the meanwhile
                    return ChunkState.DOES_NOT_EXIST;
                } else if (lockStatus == LockManager.LockStatus.TIMEOUT) {
                    // try lock did not succeed
                    return ChunkState.LOCK_TIMEOUT;
                } else {
                    throw new IllegalStateException();
                }
            }
        }

        // TODO wrap with try catch and catch memory runtime exception? are there any left to catch?
        // -> memory dump on error

        // because this is quite expensive, keep this as an assert to catch writing beyond the chunk bounds
        assert p_data.length <= m_context.getHeap().getSize(tableEntry);

        imExporter.writeBytes(p_data);

        if (!m_context.isChunkLockDisabled()) {
            LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry, p_lockOperation, p_lockTimeoutMs);
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_PUT.inc();

        return ChunkState.OK;
    }

    /**
     * Assert the lock operation used
     *
     * @param p_lockOperation Lock operation to use with the current op
     * @return True if ok, exception thrown if not supported
     */
    private boolean assertLockOperationSupport(final ChunkLockOperation p_lockOperation) {
        switch (p_lockOperation) {
            case NONE:
            case WRITE_LOCK_ACQ_PRE_OP:
            case WRITE_LOCK_REL_POST_OP:
            case WRITE_LOCK_SWAP_POST_OP:
            case WRITE_LOCK_ACQ_OP_REL:
            case WRITE_LOCK_ACQ_OP_SWAP:
            case READ_LOCK_SWAP_PRE_OP:
            case READ_LOCK_SWAP_OP_REL:
                return true;

            case WRITE_LOCK_SWAP_PRE_OP:
            case WRITE_LOCK_SWAP_OP_REL:
            case READ_LOCK_ACQ_PRE_OP:
            case READ_LOCK_REL_POST_OP:
            case READ_LOCK_SWAP_POST_OP:
            case READ_LOCK_ACQ_OP_REL:
            case READ_LOCK_ACQ_OP_SWAP:
            case WRITE_LOCK_ACQ_POST_OP:
            case READ_LOCK_ACQ_POST_OP:
                throw new MemoryRuntimeException("Unsupported lock operation on create op: " + p_lockOperation);

            default:
                throw new IllegalStateException("Unhandled lock operation");
        }
    }
}
