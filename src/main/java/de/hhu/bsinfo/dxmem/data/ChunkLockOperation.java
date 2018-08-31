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
     * Defaults to read lock on get/put operation
     */
    NONE,

    /**
     * Acquire the write lock of a chunk and keep it until it is released with another operation call
     */
    ACQUIRE_BEFORE_OP,

    /**
     * Release the write lock of a chunk. must be acquired with a previous operation before issuing this operation
     */
    RELEASE_AFTER_OP,

    /**
     * Acquire, execute operation, then release the write lock on get/put operations
     */
    ACQUIRE_OP_RELEASE
}
