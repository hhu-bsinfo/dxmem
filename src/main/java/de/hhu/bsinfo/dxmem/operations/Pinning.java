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
import de.hhu.bsinfo.dxmem.core.LockManager;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;

/**
 * Pin a chunk to allow direct access to heap data using the address (and for RDMA)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Pinning {
    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public Pinning(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Pin a chunk
     *
     * @param p_cid
     *         Cid of chunk to pin
     * @return PinnedMemory object with ChunkState determining the result of the operation
     */
    public PinnedMemory pin(final long p_cid) {
        return pin(p_cid, -1);
    }

    /**
     * Pin a chunk
     *
     * @param p_cid
     *         Cid of chunk to pin
     * @param p_acquireLockTimeoutMs
     *         -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and &gt; 0 for a timeout value in ms
     * @return PinnedMemory object with ChunkState determining the result of the operation
     */
    public PinnedMemory pin(final long p_cid, final int p_acquireLockTimeoutMs) {
        if (p_cid == ChunkID.INVALID_ID) {
            return new PinnedMemory(ChunkState.INVALID_ID);
        }

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            return new PinnedMemory(ChunkState.DOES_NOT_EXIST);
        }

        if (!m_context.isChunkLockDisabled()) {
            LockManager.LockStatus lockStatus = LockManager.executeBeforeOp(m_context.getCIDTable(), tableEntry,
                    ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, p_acquireLockTimeoutMs);

            // acquire write lock to ensure the chunk is not deleted while trying to pin it
            if (lockStatus != LockManager.LockStatus.OK) {
                m_context.getDefragmenter().releaseApplicationThreadLock();

                return new PinnedMemory(ChunkState.DOES_NOT_EXIST);
            }
        }

        tableEntry.setPinned(true);

        m_context.getCIDTable().entryUpdate(tableEntry);

        if (!m_context.isChunkLockDisabled()) {
            LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry,
                    ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1);
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return new PinnedMemory(tableEntry.getAddress());
    }

    /**
     * Unpin a pinned chunk. Depending on how many chunks are currently stored, this call is very slow because it
     * has to perform a depth search on the CIDTable to find the CIDTable entry
     *
     * @param p_pinnedChunkAddress
     *         Address of pinned chunk
     * @return CID of unpinned chunk on success, INVALID_ID on failure
     */
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

        if (!m_context.isChunkLockDisabled()) {
            LockManager.LockStatus lockStatus = LockManager.executeBeforeOp(m_context.getCIDTable(), tableEntry,
                    ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, -1);

            // acquire write lock to ensure the chunk is not deleted while trying to pin it
            if (lockStatus != LockManager.LockStatus.OK) {
                m_context.getDefragmenter().releaseApplicationThreadLock();

                throw new IllegalStateException("Pinned chunk with CID " + ChunkID.toHexString(cid) +
                        " with chunk table entry " + tableEntry + " deleted while waiting for write lock");
            }
        }

        tableEntry.setPinned(false);

        m_context.getCIDTable().entryUpdate(tableEntry);

        if (!m_context.isChunkLockDisabled()) {
            LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry,
                    ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1);
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return cid;
    }

    /**
     * Unpin a pinned chunk using the CID
     *
     * @param p_cidOfPinnedChunk
     *         CID of pinned chunk
     */
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

        if (!m_context.isChunkLockDisabled()) {
            LockManager.LockStatus lockStatus = LockManager.executeBeforeOp(m_context.getCIDTable(), tableEntry,
                    ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, -1);

            // acquire write lock to ensure the chunk is not deleted while trying to pin it
            if (lockStatus != LockManager.LockStatus.OK) {
                m_context.getDefragmenter().releaseApplicationThreadLock();

                throw new IllegalStateException("Pinned chunk " + ChunkID.toHexString(p_cidOfPinnedChunk) +
                        " deleted while waiting for write lock");
            }
        }

        tableEntry.setPinned(false);

        m_context.getCIDTable().entryUpdate(tableEntry);

        LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry,
                ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1);

        m_context.getDefragmenter().releaseApplicationThreadLock();
    }

    /**
     * Wrapper class for pinned memory data
     */
    public static final class PinnedMemory {
        private final ChunkState m_state;
        private final long m_address;

        /**
         * Constructor
         *
         * @param p_state
         *         State of chunk (result of operation)
         */
        private PinnedMemory(final ChunkState p_state) {
            m_state = p_state;
            m_address = Address.INVALID;
        }

        /**
         * Constructor
         *
         * @param p_address
         *         Address of pinned chunk
         */
        private PinnedMemory(final long p_address) {
            m_state = ChunkState.OK;
            m_address = p_address;
        }

        /**
         * Get the state of the chunk (result of last operation)
         *
         * @return ChunkState
         */
        public ChunkState getState() {
            return m_state;
        }

        /**
         * Get the address of the pinned chunk
         *
         * @return Address of pinned chunk
         */
        public long getAddress() {
            return m_address;
        }

        /**
         * Is chunk state ok (quick check on no errors)
         *
         * @return True on no errors, false otherwise
         */
        public boolean isStateOk() {
            return m_state == ChunkState.OK;
        }
    }
}
