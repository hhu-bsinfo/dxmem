package de.hhu.bsinfo.dxmem.data;

/**
 * State object for chunk lock status
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.11.2018
 */
public class ChunkLockState {
    private final long m_cid;
    private boolean m_writeLock;
    private final int m_readLockCount;

    /**
     * Constructor
     *
     * @param p_cid Cid of chunk
     * @param p_writeLock Write lock set
     * @param p_readLockCount Number of read locks set
     */
    public ChunkLockState(final long p_cid, final boolean p_writeLock, final int p_readLockCount) {
        m_cid = p_cid;
        m_writeLock = p_writeLock;
        m_readLockCount = p_readLockCount;
    }

    /**
     * Get the cid of the chunk
     *
     * @return Cid
     */
    public long getCid() {
        return m_cid;
    }

    /**
     * Is the chunk write locked
     *
     * @return True if write locked, false otherwise
     */
    public boolean isWriteLocked() {
        return m_writeLock;
    }

    /**
     * Is the chunk read locked
     *
     * @return True if read locked, false otherwise
     */
    public boolean isReadLocked() {
        return m_readLockCount > 0;
    }

    /**
     * Get the number of read locks acquired
     *
     * @return Number of read locks
     */
    public int getReadLockCount() {
        return m_readLockCount;
    }

    @Override
    public String toString() {
        return String.format("%X: w %d | r %d", m_cid, m_writeLock ? 1 : 0, m_readLockCount);
    }
}
