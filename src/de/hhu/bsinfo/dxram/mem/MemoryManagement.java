package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.mem.exceptions.MemoryRuntimeException;
import de.hhu.bsinfo.dxram.mem.exceptions.OutOfKeyValueStoreMemoryException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.STATE_NOT_MOVEABLE;
import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.STATE_NOT_REMOVEABLE;

/**
 * Managing memory accesses for creating and deleting objects
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 28.02.18
 * @projectname dxram-memory
 */
public class MemoryManagement {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MemoryManagement.class.getSimpleName());

    private final MemoryManager memory;
    private final SmallObjectHeap smallObjectHeap;
    private final CIDTable cidTable;
    private final short NODE_ID;

    private AtomicInteger m_lock;

    /**
     * Constructor
     *
     * @param memoryManager
     *          The central unit which manages all memory accesses
     *
     */
    MemoryManagement(MemoryManager memoryManager) {
        memory = memoryManager;
        smallObjectHeap = memoryManager.smallObjectHeap;
        cidTable = memoryManager.cidTable;

        NODE_ID = cidTable.m_ownNodeID;

        m_lock = new AtomicInteger(0);
    }
    
    /**
     * The chunk ID 0 is reserved for a fixed index structure.
     * If the index structure is already created this will delete the old
     * one and allocate a new block of memory with the same id (0).
     *
     * This method is Thread-Safe
     *
     * @param p_size
     *         Size for the index chunk.
     * @return The chunk id 0
     */
    long createIndex(final int p_size) throws OutOfKeyValueStoreMemoryException, MemoryRuntimeException {
        assert p_size > 0;

        long directEntryAddress;
        long entry;
        long chunkID;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createIndex p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        try {
            //get a management lock
            lockManage();

            directEntryAddress = cidTable.getAddressOfEntryCreate(0);
            if (cidTable.directGet(directEntryAddress) != 0) {
                // delete old entry
                entry = cidTable.directDelete(directEntryAddress, false);

                smallObjectHeap.free(entry);
                memory.info.totalActiveChunkMemory -= smallObjectHeap.getSizeDataBlock(entry);
                memory.info.numActiveChunks--;
            }

            entry = smallObjectHeap.malloc(p_size);
            if (entry > SmallObjectHeap.INVALID_ADDRESS) {
                //->chunkID = (long) m_boot.getNodeID() << 48;
                chunkID = (long) NODE_ID << 48; //<<

                // register new chunk in cid table
                if (!cidTable.directSet(directEntryAddress, entry)) {
                    // on demand allocation of new table failed
                    // free previously created chunk for data to avoid memory leak
                    smallObjectHeap.free(entry);
                    throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
                } else {
                    memory.info.numActiveChunks++;
                    memory.info.totalActiveChunkMemory += p_size;
                }
            } else {
                throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
            }
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
            throw e;
        } finally {
            //do in any case a unlock
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createIndex p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        return chunkID;
    }

    /**
     * Create a new chunk.
     *
     * This method is Thread-Safe
     *
     * @param p_size
     *         Size in bytes of the payload the chunk contains.
     * @return Chunk ID for the allocated chunk
     */

    long create(final int p_size) throws OutOfKeyValueStoreMemoryException, MemoryRuntimeException {
        assert p_size > 0;

        long chunkID = ChunkID.INVALID_ID;
        long lid;

        long entry;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER create p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        try {
            //get a management lock
            lockManage();

            // #ifdef STATISTICS
            //->SOP_CREATE.enter();
            // #endif /* STATISTICS */

            // get new LID from CIDTable
            lid = cidTable.getFreeLID();

            // first, try to allocate. maybe early return
            // #ifdef STATISTICS
            //->SOP_MALLOC.enter(p_size);
            // #endif /* STATISTICS */
            entry= smallObjectHeap.malloc(p_size);
            // #ifdef STATISTICS
            //->SOP_MALLOC.leave();
            // #endif /* STATISTICS */
            if (entry != SmallObjectHeap.INVALID_ADDRESS) {
                //->chunkID = ((long) m_boot.getNodeID() << 48) + lid;
                chunkID = ((long) NODE_ID << 48) + lid;//<<

                // register new chunk in cid table
                if (!cidTable.setAndCreate(chunkID, entry)) {
                    // on demand allocation of new table failed
                    // free previously created chunk for data to avoid memory leak
                    smallObjectHeap.free(entry);

                    //LOGGER.warn("OutOfKeyValueStoreMemory");
                    //throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
                } else {
                    memory.info.numActiveChunks++;
                    memory.info.totalActiveChunkMemory += p_size;
                }
            } else {
                // put lid back
                cidTable.putChunkIDForReuse(lid);
                //LOGGER.warn("OutOfKeyValueStoreMemory");
                //throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_CREATE.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
            throw e;
        } finally {
            //do in any case a unlock
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT create p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        return chunkID;
    }

    /**
     * Create a chunk with a specific chunk id (used for migration/recovery).
     *
     * This method is Thread-Safe
     *
     * @param p_chunkId
     *         Chunk id to assign to the chunk.
     * @param p_size
     *         Size of the chunk.
     * @return The chunk id if successful, -1 if another chunk with the same id already exists.
     */
    long create(final long p_chunkId, final int p_size) throws OutOfKeyValueStoreMemoryException, MemoryRuntimeException {
        assert p_size > 0;

        long directEntryAddress;

        long chunkID = ChunkID.INVALID_ID;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER create p_chunkId 0x%X, p_size %d", p_chunkId, p_size);
        // #endif /* LOGGER == TRACE */

        try {
            //get a management lock
            lockManage();

            // #ifdef STATISTICS
            //->SOP_CREATE.enter();
            // #endif /* STATISTICS */

            // verify this id is not used
            directEntryAddress = cidTable.getAddressOfEntryCreate(p_chunkId);
            long entry = cidTable.directGet(directEntryAddress);
            if (entry == CIDTable.ZOMBIE_ENTRY || entry == CIDTable.FREE_ENTRY) {
                entry = smallObjectHeap.malloc(p_size);
                if (entry != SmallObjectHeap.INVALID_ADDRESS) {
                    // register new chunk
                    // register new chunk in cid table
                    if (!cidTable.directSet(directEntryAddress, entry)) {
                        // on demand allocation of new table failed
                        // free previously created chunk for data to avoid memory leak
                        smallObjectHeap.free(entry);
                        throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
                    } else {
                        memory.info.numActiveChunks++;
                        memory.info.totalActiveChunkMemory += p_size;
                        chunkID = p_chunkId;
                    }
                } else {
                    throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
                }
            }

            // #ifdef STATISTICS
            //->SOP_CREATE.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
            throw e;
        } finally {
            //do in any case a unlock
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT create p_chunkId 0x%X, p_size %d", p_chunkId, p_size);
        // #endif /* LOGGER == TRACE */

        return chunkID;
    }

    /**
     * Batch/Multi create with a list of sizes
     *
     * This method is Thread-Safe
     *
     * @param p_sizes
     *         List of sizes to create chunks for
     * @return List of chunk ids matching the order of the size list
     */
    long[] createMultiSizes(final int... p_sizes) {
        return createMultiSizes(false, p_sizes);
    }

    /**
     * Batch/Multi create with a list of sizes
     *
     * This method is Thread-Safe
     *
     * @param p_consecutive
     *         True to enforce consecutive chunk ids
     * @param p_sizes
     *         List of sizes to create chunks for
     * @return List of chunk ids matching the order of the size list
     */
     long[] createMultiSizes(final boolean p_consecutive, final int... p_sizes) {
        long[] entries;
        long[] lids;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createMultiSizes p_consecutive %b, p_sizes %d", p_consecutive, p_sizes.length);
        // #endif /* LOGGER == TRACE */

        try {
            //get a management lock
            lockManage();

            // #ifdef STATISTICS
            //->SOP_MULTI_CREATE.enter();
            // #endif /* STATISTICS */

            // get new LIDs
            lids = cidTable.getFreeLIDs(p_sizes.length, p_consecutive);

            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.enter(p_sizes.length);
            // #endif /* STATISTICS */
            entries = smallObjectHeap.multiMallocSizes(p_sizes);
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.leave();
            // #endif /* STATISTICS */
            if (entries != null) {

                for (int i = 0; i < lids.length; i++) {
                    //->lids[i] = ((long) m_boot.getNodeID() << 48) + lids[i];
                    lids[i] = ((long) NODE_ID << 48) + lids[i];//<<

                    // register new chunk in cid table
                    if (!cidTable.setAndCreate(lids[i], entries[i])) {
                        for (int j = i; j >= 0; j--) {
                            // on demand allocation of new table failed
                            // free previously created chunk for data to avoid memory leak
                            smallObjectHeap.free(entries[j]);
                        }

                        throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
                    } else {
                        memory.info.numActiveChunks++;
                        memory.info.totalActiveChunkMemory += p_sizes[i];
                    }
                }

            } else {
                // put lids back
                for (long lid : lids) {
                    cidTable.putChunkIDForReuse(lid);
                }

                throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_MULTI_CREATE.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
            throw e;
        } finally {
            //do in any case a unlock
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createMultiSizes p_consecutive %b, p_sizes %d", p_consecutive, p_sizes.length);
        // #endif /* LOGGER == TRACE */

        return lids;
    }

    /**
     * Batch/Multi create with a list of data structures
     *
     * This method is Thread-Safe
     *
     * @param p_dataStructures
     *         List of data structures. Chunk ids are automatically assigned after creation
     */
    void createMulti(final DataStructure... p_dataStructures) {
        createMulti(false, p_dataStructures);
    }

    /**
     * Batch/Multi create with a list of data structures
     *
     * This method is Thread-Safe
     *
     * @param p_consecutive
     *         True to enforce consecutive chunk ids
     * @param p_dataStructures
     *         List of data structures. Chunk ids are automatically assigned after creation
     */
    void createMulti(final boolean p_consecutive, final DataStructure... p_dataStructures) {
        int[] sizes = new int[p_dataStructures.length];

        for (int i = 0; i < p_dataStructures.length; i++) {
            sizes[i] = p_dataStructures[i].sizeofObject();
        }

        long[] ids = createMultiSizes(p_consecutive, sizes);

        for (int i = 0; i < ids.length; i++) {
            p_dataStructures[i].setID(ids[i]);
        }
    }

    /**
     * Batch create chunks
     *
     * @param p_size
     *         Size of the chunks
     * @param p_count
     *         Number of chunks with the specified size
     * @return Chunk id list of the created chunks
     */
    long[] createMulti(final int p_size, final int p_count) {
        return createMulti(p_size, p_count, false);
    }

    /**
     * Batch create chunks
     *
     * @param p_size
     *         Size of the chunks
     * @param p_count
     *         Number of chunks with the specified size
     * @param p_consecutive
     *         True to enforce consecutive chunk ids
     * @return Chunk id list of the created chunks
     */
    long[] createMulti(final int p_size, final int p_count, final boolean p_consecutive) {
        long[] entries;
        long[] lids;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createMultiSizes p_size %d, p_count %d, p_consecutive %b", p_size, p_count, p_consecutive);
        // #endif /* LOGGER == TRACE */

        try {
            //get a management lock
            lockManage();

            // #ifdef STATISTICS
            //->SOP_MULTI_CREATE.enter();
            // #endif /* STATISTICS */

            // get new LIDs
            lids = cidTable.getFreeLIDs(p_count, p_consecutive);

            // first, try to allocate. maybe early return
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.enter(p_size);
            // #endif /* STATISTICS */
            entries = smallObjectHeap.multiMalloc(p_size, p_count);
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.leave();
            // #endif /* STATISTICS */
            if (entries != null) {

                for (int i = 0; i < lids.length; i++) {
                    //->lids[i] = ((long) m_boot.getNodeID() << 48) + lids[i];
                    lids[i] = ((long) NODE_ID << 48) + lids[i];//<<

                    // register new chunk in cid table
                    if (!cidTable.setAndCreate(lids[i], entries[i])) {

                        for (int j = i; j >= 0; j--) {
                            // on demand allocation of new table failed
                            // free previously created chunk for data to avoid memory leak
                            smallObjectHeap.free(entries[j]);
                        }

                        throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
                    } else {
                        memory.info.numActiveChunks++;
                        memory.info.totalActiveChunkMemory += p_size;
                    }
                }

            } else {
                // put lids back
                for (long lid : lids) {
                    cidTable.putChunkIDForReuse(lid);
                }

                throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_MULTI_CREATE.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
            throw e;
        } finally {
            //do in any case a unlock
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createMultiSizes p_size %d, p_count %d, p_consecutive %b", p_size, p_count, p_consecutive);
        // #endif /* LOGGER == TRACE */

        return lids;
    }

    /**
     * Special create and put call optimized for recovery
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_chunkIDs
     *         List of recovered chunk ids
     * @param p_data
     *         Recovered data
     * @param p_offsets
     *         Offset list for chunks to address the data array
     * @param p_lengths
     *         List of chunk sizes
     * @param p_usedEntries
     *         Specifies the actual number of slots used in the array (may be less than p_lengths)
     */
    void createAndPutRecovered(final long[] p_chunkIDs, final byte[] p_data, final int[] p_offsets, final int[] p_lengths, final int p_usedEntries) {
        long[] entries;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createAndPutRecovered, count %d", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        try {
            //get a management lock
            lockManage();

            // #ifdef STATISTICS
            //->SOP_CREATE_PUT_RECOVERED.enter(p_usedEntries);
            // #endif /* STATISTICS */

            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.enter(p_usedEntries);
            // #endif /* STATISTICS */
            entries = smallObjectHeap.multiMallocSizesUsedEntries(p_usedEntries, p_lengths);
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.leave();
            // #endif /* STATISTICS */
            if (entries != null) {

                for (int i = 0; i < entries.length; i++) {
                    smallObjectHeap.writeBytes(entries[i], 0, p_data, p_offsets[i], p_lengths[i]);
                    memory.info.totalActiveChunkMemory += p_lengths[i];
                }

                memory.info.numActiveChunks += entries.length;

                for (int i = 0; i < entries.length; i++) {
                    cidTable.setAndCreate(p_chunkIDs[i], entries[i]);
                }
            } else {
                throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_CREATE_PUT_RECOVERED.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
            throw e;
        } finally {
            //do in any case a unlock
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createAndPutRecovered, count %d", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Special create and put call optimized for recovery
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_dataStructures
     *         All data structure to create and put
     * @return number of written bytes
     */
    int createAndPutRecovered(final DataStructure... p_dataStructures) {
        int ret = 0;
        long[] entries;
        int[] sizes = new int[p_dataStructures.length];

        for (int i = 0; i < p_dataStructures.length; i++) {
            sizes[i] = p_dataStructures[i].sizeofObject();
        }

        try {
            //get a management lock
            lockManage();

            // #ifdef STATISTICS
            //->SOP_CREATE_PUT_RECOVERED.enter(p_dataStructures.length);
            // #endif /* STATISTICS */

            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.enter(p_dataStructures.length);
            // #endif /* STATISTICS */
            entries = smallObjectHeap.multiMallocSizes(sizes);
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.leave();
            // #endif /* STATISTICS */
            if (entries != null) {

                for (int i = 0; i < entries.length; i++) {
                    SmallObjectHeapDataStructureImExporter exporter = memory.getImExporter(entries[i]);
                    exporter.exportObject(p_dataStructures[i]);
                    ret += sizes[i];
                    memory.info.totalActiveChunkMemory += sizes[i];
                }

                memory.info.numActiveChunks += entries.length;

                for (int i = 0; i < entries.length; i++) {
                    cidTable.set(p_dataStructures[i].getID(), entries[i]);
                }
            } else {
                throw new OutOfKeyValueStoreMemoryException(memory.info.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_CREATE_PUT_RECOVERED.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
            throw e;
        } finally {
            //do in any case a unlock
            unlockManage();
        }

        return ret;
    }

    /**
     * Removes a Chunk from the memory
     * This method use a switchable lock. //TODO Reset to normal lock
     *
     * @param p_chunkID
     *         the ChunkID of the Chunk
     * @param p_wasMigrated
     *         default value for this parameter should be false!
     *         if chunk was deleted during migration this flag should be set to true
     * @return The size of the deleted chunk if removing the data was successful, -1 if the chunk with the specified id does not exist
     */
    int remove(final long p_chunkID, final boolean p_wasMigrated) {
        int ret = -1;
        long directEntryAddress;
        long entry;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER remove p_chunkID 0x%X, p_wasMigrated %d", p_chunkID, p_wasMigrated);
        // #endif /* LOGGER == TRACE */

        if (p_chunkID != ChunkID.INVALID_ID &&
                (directEntryAddress = cidTable.getAddressOfEntry(p_chunkID)) != SmallObjectHeap.INVALID_ADDRESS &&
                memory.switchableWriteLock(directEntryAddress)) {
            try {
                //get a management lock
                lockManage();

                // #ifdef STATISTICS
                //->SOP_REMOVE.enter();
                // #endif /* STATISTICS */

                // Get and delete the address from the CIDTable, mark as zombie first
                entry = cidTable.directGet(directEntryAddress);
                if(STATE_NOT_MOVEABLE.get(entry) || STATE_NOT_REMOVEABLE.get(entry)){
                    unlockManage();
                    LOGGER.info("CID: %d is not remove able!!!!", p_chunkID);
                    return 0;
                }

                if(entry == CIDTable.ZOMBIE_ENTRY || entry == CIDTable.FREE_ENTRY) {
                    unlockManage();
                } else {
                    cidTable.directSet(directEntryAddress, CIDTable.ZOMBIE_ENTRY);

                    if (p_wasMigrated) {
                        // deleted and previously migrated chunks don't end up in the LID store
                        cidTable.set(p_chunkID, CIDTable.FREE_ENTRY);
                    } else {
                        // more space for another zombie for reuse in LID store?
                        if (cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID))) {
                            // kill zombie entry
                            cidTable.set(p_chunkID, CIDTable.FREE_ENTRY);
                        } else {
                            // no space for zombie in LID store, keep him "alive" in table
                        }
                    }
                    ret = smallObjectHeap.getSizeDataBlock(entry);
                    //System.out.println(String.format("0x%X , %d\n%s", entry, ret, CIDTableEntry.entryData(entry)));
                    // #ifdef STATISTICS
                    //->SOP_FREE.enter(ret);
                    // #endif /* STATISTICS */
                    smallObjectHeap.free(entry);
                    // #ifdef STATISTICS
                    //->SOP_FREE.leave();
                    // #endif /* STATISTICS */
                    memory.info.numActiveChunks--;
                    memory.info.totalActiveChunkMemory -= ret;
                }

            } catch (final MemoryRuntimeException e) {
                MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                throw e;
            } finally {
                //do in any case a unlock
                memory.switchableWriteUnlock(directEntryAddress);
                unlockManage();
            }
        }


        // #if LOGGER == TRACE
        LOGGER.trace("EXIT remove p_chunkID 0x%X, p_wasMigrated %d", p_chunkID, p_wasMigrated);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Lock the memory for a management task (create, remove).
     */
    private void lockManage() {
        do {
            int v = m_lock.get();
            m_lock.compareAndSet(v, v | 0x40000000);
        } while (!m_lock.compareAndSet(0x40000000, 0x80000000));
    }

    /**
     * Unlock the memory after a management task (create, put, remove).
     */
    private void unlockManage() {
        m_lock.set(0);
    }
}
