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
 * Lock operations to execute. Encode as enum to get tableswitches (jumptables) in bytecode for low over dispatch.
 *
 * Not lock operation is supported by every memory operation. Check the dedicated assertLockOperationSupport methods
 * of each operation class. Some lock operations are not supported either because it's not possible or doesn't make
 * sense.
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
     * Acquire the write lock before the operation
     */
    WRITE_LOCK_ACQ_PRE_OP,

    /**
     * Swap an already acquired write lock for a read lock before the operation
     */
    WRITE_LOCK_SWAP_PRE_OP,

    /**
     * Acquire the write lock after the operation
     */
    WRITE_LOCK_ACQ_POST_OP,

    /**
     * Release the already acquired write lock after the operation
     */
    WRITE_LOCK_REL_POST_OP,

    /**
     * Swap the already acquired write lock for a read lock after the operation
     */
    WRITE_LOCK_SWAP_POST_OP,

    /**
     * Acquire the write lock before the operation, execute the operation and then release the write lock after the
     * operation is finished
     */
    WRITE_LOCK_ACQ_OP_REL,

    /**
     * Swap an already acquired write lock for a read lock before the operation and release the read lock after
     * the operation
     */
    WRITE_LOCK_SWAP_OP_REL,

    /**
     * Acquire the write lock before the operation and swap it for a read lock after the operation is finished
     */
    WRITE_LOCK_ACQ_OP_SWAP,

    /**
     * Acquire a read lock before the operation
     */
    READ_LOCK_ACQ_PRE_OP,

    /**
     * Swap an already acquired read lock before the operation for a write lock.
     *
     * Note: A fully atomic read lock swap operation is not possible (like on the write lock swap). The swap
     * releases the read lock in a separate operation before acquiring the write lock.
     */
    READ_LOCK_SWAP_PRE_OP,

    /**
     * Acquire a read lock after the operation
     */
    READ_LOCK_ACQ_POST_OP,

    /**
     * Release an already acquired read lock after the operation
     */
    READ_LOCK_REL_POST_OP,

    /**
     * Swap an already acquired read lock after the operation for a write lock.
     *
     * Note: A fully atomic read lock swap operation is not possible (like on the write lock swap). The swap
     * releases the read lock in a separate operation before acquiring the write lock.
     */
    READ_LOCK_SWAP_POST_OP,

    /**
     * Acquire a read lock before the operation and release it after the operation is finished
     */
    READ_LOCK_ACQ_OP_REL,

    /**
     * Swap an already acquired read lock before the operation and release it after the operation is finished.
     *
     * Note: A fully atomic read lock swap operation is not possible (like on the write lock swap). The swap
     * releases the read lock in a separate operation before acquiring the write lock.
     */
    READ_LOCK_SWAP_OP_REL,

    /**
     * Acquire a read lock before the operation, execute the operation and swap the read lock after the operation is
     * finished.
     *
     * Note: A fully atomic read lock swap operation is not possible (like on the write lock swap). The swap
     * releases the read lock in a separate operation before acquiring the write lock.
     */
    READ_LOCK_ACQ_OP_SWAP,
}
