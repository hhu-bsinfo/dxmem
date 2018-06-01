package de.hhu.bsinfo.dxmem.core;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Defragmenter {
    // TODO stub for now but we need to call the locks in the code path to ensure everything's ready
    // for the future implementation

    private final boolean m_enabled;
    private final ReadWriteLock m_lock;

    public Defragmenter(final boolean p_enabled) {
        m_enabled = p_enabled;

        if (m_enabled) {
            m_lock = new ReentrantReadWriteLock(false);
        } else {
            m_lock = null;
        }
    }

    // read lock needs to be acquired by every operation to allow the defragmenter
    // to block all external (application thread) access to the CIDTable AND the Heap when
    // executing
    public void acquireApplicationThreadLock() {
        // don't use any locks if defragmenter disabled which speeds up things
        if (!m_enabled) {
            return;
        }

        m_lock.readLock().lock();
    }

    public void releaseApplicationThreadLock() {
        if (!m_enabled) {
            return;
        }

        m_lock.readLock().unlock();
    }
}
