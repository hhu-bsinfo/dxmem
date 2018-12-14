package de.hhu.bsinfo.dxmem.operations;

import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.core.LockManager;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkLockState;
import de.hhu.bsinfo.dxmem.data.ChunkState;

/**
 * Separate lock operation to lock/unlock chunks
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.11.2018
 */
public class Lock {
    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context
     */
    public Lock(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Get the current lock status of a chunk
     *
     * @param p_chunk
     *         Chunk to get lock status of
     * @return Lock status of chunk
     */
    public ChunkLockState status(final AbstractChunk p_chunk) {
        ChunkLockState state = status(p_chunk.getID());

        if (state == null) {
            p_chunk.setState(ChunkState.DOES_NOT_EXIST);
        } else {
            p_chunk.setState(ChunkState.OK);
        }

        return state;
    }

    /**
     * Get the current lock status of a chunk
     *
     * @param p_cid
     *         Cid of chunk to get lock status of
     * @return Lock status of chunk
     */
    public ChunkLockState status(final long p_cid) {
        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        if (!tableEntry.isValid()) {
            return null;
        }

        return new ChunkLockState(p_cid, tableEntry.isWriteLockAcquired(),
                tableEntry.getReadLockCounter());
    }

    /**
     * Lock a chunk
     *
     * @param p_cid
     *         Cid of chunk to lock
     * @param p_writeLock
     *         Type of lock: true for write lock, false read lock
     * @param p_lockTimeoutMs
     *         If a lock operation is set, set to -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and &gt; 0 for a timeout value in ms
     * @return ChunkState of the lock operation
     */
    public ChunkState lock(final long p_cid, final boolean p_writeLock, final int p_lockTimeoutMs) {
        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            return ChunkState.DOES_NOT_EXIST;
        }

        LockManager.LockStatus lockStatus = LockManager.LockStatus.OK;

        if (!m_context.isChunkLockDisabled()) {
            if (!p_writeLock) {
                lockStatus = LockManager.executeBeforeOp(m_context.getCIDTable(), tableEntry,
                        ChunkLockOperation.READ_LOCK_ACQ_PRE_OP, p_lockTimeoutMs);
            } else {
                lockStatus = LockManager.executeBeforeOp(m_context.getCIDTable(), tableEntry,
                        ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP, p_lockTimeoutMs);
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        switch (lockStatus) {
            case OK:
                return ChunkState.OK;
            case INVALID:
                // entry was deleted in the meanwhile
                return ChunkState.DOES_NOT_EXIST;
            case TIMEOUT:
                return ChunkState.LOCK_TIMEOUT;
            default:
                throw new IllegalStateException("Unhandled switch case");
        }
    }

    /**
     * Lock a chunk
     *
     * @param p_chunk
     *         Chunk to lock
     * @param p_writeLock
     *         Type of lock: true for write lock, false read lock
     * @param p_lockTimeoutMs
     *         If a lock operation is set, set to -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and &gt; 0 for a timeout value in ms
     * @return True on success, false on failure. Chunk state with additional information is set in p_chunk
     */
    public boolean lock(final AbstractChunk p_chunk, final boolean p_writeLock, final int p_lockTimeoutMs) {
        ChunkState state = lock(p_chunk.getID(), p_writeLock, p_lockTimeoutMs);
        p_chunk.setState(state);
        return p_chunk.isStateOk();
    }

    /**
     * Unlock a chunk
     *
     * @param p_cid
     *         Cid of chunk to unlock
     * @param p_writeLock
     *         Type of lock: true for write lock, false read lock
     * @return ChunkState of the lock operation
     */
    public ChunkState unlock(final long p_cid, final boolean p_writeLock) {
        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            return ChunkState.DOES_NOT_EXIST;
        }

        LockManager.LockStatus status = LockManager.LockStatus.OK;

        if (!m_context.isChunkLockDisabled()) {
            if (!p_writeLock) {
                status = LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry,
                        ChunkLockOperation.READ_LOCK_REL_POST_OP, -1);
            } else {
                status = LockManager.executeAfterOp(m_context.getCIDTable(), tableEntry,
                        ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1);
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        switch (status) {
            case OK:
                return ChunkState.OK;
            case INVALID:
                // entry was deleted in the meanwhile
                return ChunkState.DOES_NOT_EXIST;
            case TIMEOUT:
                return ChunkState.LOCK_TIMEOUT;
            default:
                throw new IllegalStateException("Unhandled switch case");
        }
    }

    /**
     * Unlock a chunk
     *
     * @param p_chunk
     *         Chunk to unlock
     * @param p_writeLock
     *         Type of lock: true for write lock, false read lock
     * @return True on success, false on failure. Chunk state with additional information is set in p_chunk
     */
    public boolean unlock(final AbstractChunk p_chunk, final boolean p_writeLock) {
        ChunkState state = unlock(p_chunk.getID(), p_writeLock);
        p_chunk.setState(state);
        return p_chunk.isStateOk();
    }

    /**
     * Swap an acquired read lock for a write lock or an acquired write lock for a read lock
     *
     * @param p_cid
     *         CID of chunk for lock swap
     * @param p_swapToWriteLock
     *         True to swap from a read lock to a write lock, false for vice versa
     * @param p_timeoutMs
     *         If a lock operation is set, set to -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and &gt; 0 for a timeout value in ms
     * @return ChunkState of the lock operation
     */
    public ChunkState swap(final long p_cid, final boolean p_swapToWriteLock, final int p_timeoutMs) {
        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getCIDTable().translate(p_cid, tableEntry);

        if (!tableEntry.isValid()) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            return ChunkState.DOES_NOT_EXIST;
        }

        LockManager.LockStatus status = LockManager.LockStatus.OK;

        if (!m_context.isChunkLockDisabled()) {
            if (p_swapToWriteLock) {
                status = LockManager.executeBeforeOp(m_context.getCIDTable(), tableEntry,
                        ChunkLockOperation.READ_LOCK_SWAP_PRE_OP, p_timeoutMs);
            } else {
                status = LockManager.executeBeforeOp(m_context.getCIDTable(), tableEntry,
                        ChunkLockOperation.WRITE_LOCK_SWAP_PRE_OP, p_timeoutMs);
            }
        }

        m_context.getDefragmenter().releaseApplicationThreadLock();

        switch (status) {
            case OK:
                return ChunkState.OK;
            case INVALID:
                // entry was deleted in the meanwhile
                return ChunkState.DOES_NOT_EXIST;
            case TIMEOUT:
                return ChunkState.LOCK_TIMEOUT;
            default:
                throw new IllegalStateException("Unhandled switch case");
        }
    }

    /**
     * Swap an acquired read lock for a write lock or an acquired write lock for a read lock
     *
     * @param p_chunk
     *         Chunk for lock swap
     * @param p_swapToWriteLock
     *         True to swap from a read lock to a write lock, false for vice versa
     * @param p_timeoutMs
     *         If a lock operation is set, set to -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and &gt; 0 for a timeout value in ms
     * @return True on success, false on failure. Chunk state with additional information is set in p_chunk
     */
    public boolean swap(final AbstractChunk p_chunk, final boolean p_swapToWriteLock, final int p_timeoutMs) {
        ChunkState state = swap(p_chunk.getID(), p_swapToWriteLock, p_timeoutMs);
        p_chunk.setState(state);
        return p_chunk.isStateOk();
    }
}
