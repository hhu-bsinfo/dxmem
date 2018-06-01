package de.hhu.bsinfo.dxmem.data;

public enum ChunkLockOperation {
    // defaults to read lock on get/put operation
    NONE,
    // acquire the write lock of a chunk and keep it until it is released with another operation call
    ACQUIRE_BEFORE_OP,
    // release the write lock of a chunk. must be acquired with a previous operation before issuing this operation
    RELEASE_AFTER_OP,
    // acquire, execute operation, then release the write lock on get/put operations
    ACQUIRE_OP_RELEASE
}
