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

import de.hhu.bsinfo.dxmem.AllocationException;
import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.LockManager;
import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Allocate memory for an already reserved CID (reserved using the Reserve operation). This can also be
 * used by the recovery to write recovered (non local chunks).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public class CreateReserved {
    private static final ValuePool SOP_CREATE_RESERVE = new ValuePool(DXMem.class, "CreateReserve");
    private static final ValuePool SOP_CREATE_RESERVE_MULTI = new ValuePool(DXMem.class, "CreateReserveMulti");

    static {
        StatisticsManager.get().registerOperation(DXMem.class, SOP_CREATE_RESERVE);
        StatisticsManager.get().registerOperation(DXMem.class, SOP_CREATE_RESERVE_MULTI);
    }

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public CreateReserved(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Allocate memory for a reserved CID. DO NOT pass arbitrary CIDs to this function. Always use the Reserve operation
     * to get CIDs which can be used with this operation. Using arbitrary CIDs will definitely break the subsystem.
     *
     * @param p_cid
     *         Reserved CID to allocate memory for
     * @param p_size
     *         Size of the chunk to allocate
     */
    public void createReserved(final long p_cid, final int p_size) {
        createReserved(p_cid, p_size, ChunkLockOperation.NONE);
    }

    /**
     * Allocate memory for a reserved CID. DO NOT pass arbitrary CIDs to this function. Always use the Reserve operation
     * to get CIDs which can be used with this operation. Using arbitrary CIDs will definitely break the subsystem.
     *
     * @param p_cid
     *         Reserved CID to allocate memory for
     * @param p_size
     *         Size of the chunk to allocate
     * @param p_lockOperation
     *         Lock operation to execute right after the chunk is created
     */
    public void createReserved(final long p_cid, final int p_size, final ChunkLockOperation p_lockOperation) {
        assert assertLockOperationSupport(p_lockOperation);
        assert p_cid != ChunkID.INVALID_ID;
        assert p_size > 0;

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        if (!m_context.getHeap().malloc(p_size, tableEntry)) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new AllocationException(p_size);
        }

        if (!m_context.getCIDTable().insert(p_cid, tableEntry)) {
            m_context.getHeap().free(tableEntry);

            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new AllocationException("Allocation of block of memory for LID table failed. Out of memory.");
        }

        LockManager.LockStatus status = LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry,
                p_lockOperation, -1);

        // this should never fail because the chunk was just created and the defragmentation thread lock is still
        // acquired
        if (status != LockManager.LockStatus.OK) {
            throw new IllegalStateException("Executing lock operation after create op " + p_lockOperation +
                    " for cid " + ChunkID.toHexString(p_cid) + " failed: " + status);
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_CREATE_RESERVE.inc();
    }

    /**
     * Allocate memory for a reserved CID. DO NOT pass arbitrary CIDs to this function. Always use the Reserve operation
     * to get CIDs which can be used with this operation. Using arbitrary CIDs will definitely break the subsystem.
     * Exception: Recovery of non local chunks
     *
     * @param p_cids
     *         Array of reserved (or recovered) CIDs to allocate memory for
     * @param p_addresses
     *         Optional (can be null): Array to return the raw addresses of the allocate chunks
     * @param p_sizes
     *         Sizes to allocate chunks for
     * @param p_sizesOffset
     *         Start offset in size array
     * @param p_sizesLength
     *         Number of elements to consider of size array
     * @return Number of chunks successfully created
     */
    public int createReserved(final long[] p_cids, final long[] p_addresses, final int[] p_sizes,
            final int p_sizesOffset, final int p_sizesLength) {
        return createReserved(p_cids, p_addresses, p_sizes, p_sizesOffset, p_sizesLength, ChunkLockOperation.NONE);
    }

    /**
     * Allocate memory for a reserved CID. DO NOT pass arbitrary CIDs to this function. Always use the Reserve operation
     * to get CIDs which can be used with this operation. Using arbitrary CIDs will definitely break the subsystem.
     * Exception: Recovery of non local chunks
     *
     * @param p_cids
     *         Array of reserved (or recovered) CIDs to allocate memory for
     * @param p_addresses
     *         Optional (can be null): Array to return the raw addresses of the allocate chunks
     * @param p_sizes
     *         Sizes to allocate chunks for
     * @param p_sizesOffset
     *         Start offset in size array
     * @param p_sizesLength
     *         Number of elements to consider of size array
     * @param p_lockOperation
     *         Lock operation to execute for every chunk right after the chunks are created
     * @return Number of chunks successfully created
     */
    public int createReserved(final long[] p_cids, final long[] p_addresses, final int[] p_sizes,
            final int p_sizesOffset, final int p_sizesLength, final ChunkLockOperation p_lockOperation) {
        assert assertLockOperationSupport(p_lockOperation);
        assert p_cids != null;
        // p_addresses is optional
        assert p_sizes != null;
        assert p_sizesOffset >= 0;
        assert p_sizesLength >= 0;

        // can't use thread local pool here
        CIDTableChunkEntry[] entries = new CIDTableChunkEntry[p_sizesLength];

        for (int i = 0; i < entries.length; i++) {
            entries[i] = new CIDTableChunkEntry();
        }

        m_context.getDefragmenter().acquireApplicationThreadLock();

        int successfulMallocs = m_context.getHeap().malloc(entries, p_sizes, p_sizesOffset, p_sizesLength);

        // add all entries to table
        for (int i = 0; i < successfulMallocs; i++) {
            if (!m_context.getCIDTable().insert(p_cids[i], entries[i])) {
                // revert mallocs for remaining chunks to avoid corrupted memory
                for (int j = i; j < successfulMallocs; j++) {
                    m_context.getHeap().free(entries[j]);
                }

                successfulMallocs = i;
            }

            LockManager.LockStatus status = LockManager.executeAfterOp(m_context.getCIDTable(), entries[i],
                    p_lockOperation, -1);

            // this should never fail because the chunk was just created and the defragmentation thread lock is still
            // acquired
            if (status != LockManager.LockStatus.OK) {
                throw new IllegalStateException("Executing lock operation after create op " + p_lockOperation +
                        " for cid " + ChunkID.toHexString(p_cids[i]) + " failed: " + status);
            }
        }

        if (p_addresses != null) {
            for (int i = 0; i < successfulMallocs; i++) {
                p_addresses[i] = entries[i].getAddress();
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_CREATE_RESERVE_MULTI.inc();

        return successfulMallocs;
    }

    /**
     * Allocate memory for a reserved chunk. DO NOT pass arbitrary chunks with CIDs to this function.
     * Always use the Reserve operation to get CIDs which can be used with this operation.
     * Using arbitrary CIDs will definitely break the subsystem.
     * Exception: Recovery of non local chunks
     *
     * @param p_ds
     *         Array of chunks with reserved CIDs set
     * @param p_addresses
     *         Optional (can be null): Array to return the raw addresses of the allocate chunks
     * @return Number of chunks successfully created
     */
    public int createReserved(final AbstractChunk[] p_ds, final long[] p_addresses) {
        return createReserved(p_ds, p_addresses, ChunkLockOperation.NONE);
    }

    /**
     * Allocate memory for a reserved chunk. DO NOT pass arbitrary chunks with CIDs to this function.
     * Always use the Reserve operation to get CIDs which can be used with this operation.
     * Using arbitrary CIDs will definitely break the subsystem.
     * Exception: Recovery of non local chunks
     *
     * @param p_ds
     *         Array of chunks with reserved CIDs set
     * @param p_addresses
     *         Optional (can be null): Array to return the raw addresses of the allocate chunks
     * @param p_lockOperation
     *         Lock operation to execute for every chunk right after the chunks are created
     * @return Number of chunks successfully created
     */
    public int createReserved(final AbstractChunk[] p_ds, final long[] p_addresses,
            final ChunkLockOperation p_lockOperation) {
        assert assertLockOperationSupport(p_lockOperation);
        assert p_ds != null;
        // p_addresses is optional

        // can't use thread local pool here
        CIDTableChunkEntry[] entries = new CIDTableChunkEntry[p_ds.length];

        for (int i = 0; i < entries.length; i++) {
            entries[i] = new CIDTableChunkEntry();
        }

        int[] sizes = new int[p_ds.length];

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = p_ds[i].sizeofObject();
        }

        m_context.getDefragmenter().acquireApplicationThreadLock();

        int successfulMallocs = m_context.getHeap().malloc(entries, sizes, 0, sizes.length);

        // add all entries to table
        for (int i = 0; i < successfulMallocs; i++) {
            if (!m_context.getCIDTable().insert(p_ds[i].getID(), entries[i])) {
                // revert mallocs for remaining chunks to avoid corrupted memory
                for (int j = i; j < successfulMallocs; j++) {
                    m_context.getHeap().free(entries[j]);
                }

                successfulMallocs = i;
            }

            LockManager.LockStatus status = LockManager.executeAfterOp(m_context.getCIDTable(), entries[i],
                    p_lockOperation, -1);

            // this should never fail because the chunk was just created and the defragmentation thread lock is still
            // acquired
            if (status != LockManager.LockStatus.OK) {
                throw new IllegalStateException("Executing lock operation after create op " + p_lockOperation +
                        " for cid " + ChunkID.toHexString(p_ds[i].getID()) + " failed: " + status);
            }
        }

        if (p_addresses != null) {
            for (int i = 0; i < successfulMallocs; i++) {
                p_addresses[i] = entries[i].getAddress();
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_CREATE_RESERVE_MULTI.inc();

        return successfulMallocs;
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
            case WRITE_LOCK_ACQ_POST_OP:
            case READ_LOCK_ACQ_POST_OP:
                return true;

            case WRITE_LOCK_ACQ_PRE_OP:
            case WRITE_LOCK_SWAP_PRE_OP:
            case WRITE_LOCK_REL_POST_OP:
            case WRITE_LOCK_SWAP_POST_OP:
            case WRITE_LOCK_ACQ_OP_REL:
            case WRITE_LOCK_SWAP_OP_REL:
            case WRITE_LOCK_ACQ_OP_SWAP:
            case READ_LOCK_ACQ_PRE_OP:
            case READ_LOCK_SWAP_PRE_OP:
            case READ_LOCK_REL_POST_OP:
            case READ_LOCK_SWAP_POST_OP:
            case READ_LOCK_ACQ_OP_REL:
            case READ_LOCK_SWAP_OP_REL:
            case READ_LOCK_ACQ_OP_SWAP:
                throw new MemoryRuntimeException("Unsupported lock operation on create op: " + p_lockOperation);

            default:
                throw new IllegalStateException("Unhandled lock operation");
        }
    }
}
