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

package de.hhu.bsinfo.dxmem.core;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Defragmenter
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Defragmenter {
    // TODO stub for now but we need to call the locks in the code path to ensure everything's ready
    // for the future implementation

    private final boolean m_enabled;
    private final ReadWriteLock m_lock;

    /**
     * Constructor
     *
     * @param p_enabled
     *         True to enable defragmenter, false to disable
     */
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
