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

package de.hhu.bsinfo.dxmem.data;

/**
 * Lock operations to execute
 */
public enum ChunkLockOperation {
    /**
     * _NO_ locks at all. Careless and incorrect usage leads to data corruption and application crashes.
     * Aside that, if you use it correctly, it allows to to combine multiple operations preceded by a single
     * lock call with following operations not executing any locking (on the same data).
     *
     * For example:
     * get with write lock acquire
     * resize with no lock
     * put with write lock release
     */
    NONE,

    /**
     * Acquire the write lock of a chunk BEFORE the operation and keep it until it is released by another
     * lock/operation call
     */
    WRITE_LOCK_ACQ_PRE_OP,

    /**
     * Release the write lock of a chunk AFTER the operation. Requires that the write lock was previously acquired.
     */
    WRITE_LOCK_REL_POST_OP,

    /**
     * Execute the operation fully write locked by acquiring the write lock before the operation and releasing it
     * after the operation has finished
     */
    WRITE_LOCK_ACQ_OP_REL,

    /**
     * Acquire the read lock of a chunk BEFORE the operation and keep it until it is released by another
     * lock/operation call
     */
    READ_LOCK_ACQ_PRE_OP,

    /**
     * Release the read lock of a chunk AFTER the operation. Requires that the read lock was previously acquired.
     */
    READ_LOCK_REL_POST_OP,

    /**
     * Execute the operation fully read locked by acquiring the read lock before the operation and releasing it
     * after the operation has finished
     */
    READ_LOCK_ACQ_OP_REL,

    /**
     * Swap the currently acquired read lock for a write lock before executing the operation
     */
    SWAP_READ_FOR_WRITE_PRE_OP,

    /**
     * Swap the currently acquired read lock for a write lock after the operation is executed
     */
    SWAP_READ_FOR_WRITE_POST_OP,

    /**
     * Swap the currently acquired write lock for a read lock before executing the operation
     */
    SWAP_WRITE_FOR_READ_PRE_OP,

    /**
     * Swap the currently acquired write lock for a read lock after the operation is executed
     */
    SWAP_WRITE_FOR_READ_POST_OP,
}
