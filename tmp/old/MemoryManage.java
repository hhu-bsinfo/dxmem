package de.hhu.bsinfo.dxmem.old;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.core.CIDTable;
import de.hhu.bsinfo.dxmem.core.Heap;
import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxmem.core.SmallObjectHeapDataStructureImExporter;
import de.hhu.bsinfo.dxmem.operations.OutOfMemoryException;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.DataStructure;

/**
 * Managing memory accesses for creating and deleting objects
 *
 * @author Florian Hucke, florian.hucke@hhu.de, 28.02.2018
 */
public class MemoryManage {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MemoryManage.class.getSimpleName());

    private final MemoryManager memory;
    private final Heap heap;
    private final CIDTable cidTable;
    private final short NODE_ID;

    private AtomicInteger m_lock;

    /**
     * Constructor
     *
     * @param memoryManager
     *         The central unit which manages all memory accesses
     */
    MemoryManage(MemoryManager memoryManager) {
        memory = memoryManager;
        heap = memoryManager.heap;
        cidTable = memoryManager.cidTable;

        NODE_ID = cidTable.m_ownNodeID;

        m_lock = new AtomicInteger(0);
    }


    /**
     * Batch/Multi create with a list of sizes
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
            entries = heap.multiMallocSizes(p_sizes);
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
                            heap.free(entries[j]);
                        }

                        throw new OutOfMemoryException(memory.info.getStatus());
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

                throw new OutOfMemoryException(memory.info.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_MULTI_CREATE.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
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
            entries = heap.multiMalloc(p_size, p_count);
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
                            heap.free(entries[j]);
                        }

                        throw new OutOfMemoryException(memory.info.getStatus());
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

                throw new OutOfMemoryException(memory.info.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_MULTI_CREATE.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
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
    void createAndPutRecovered(final long[] p_chunkIDs, final byte[] p_data, final int[] p_offsets,
            final int[] p_lengths, final int p_usedEntries) {
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
            entries = heap.multiMallocSizesUsedEntries(p_usedEntries, p_lengths);
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.leave();
            // #endif /* STATISTICS */
            if (entries != null) {

                for (int i = 0; i < entries.length; i++) {
                    heap.writeBytes(entries[i], 0, p_data, p_offsets[i], p_lengths[i]);
                    memory.info.totalActiveChunkMemory += p_lengths[i];
                }

                memory.info.numActiveChunks += entries.length;

                for (int i = 0; i < entries.length; i++) {
                    cidTable.setAndCreate(p_chunkIDs[i], entries[i]);
                }
            } else {
                throw new OutOfMemoryException(memory.info.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_CREATE_PUT_RECOVERED.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
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
            entries = heap.multiMallocSizes(sizes);
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
                throw new OutOfMemoryException(memory.info.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_CREATE_PUT_RECOVERED.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
            throw e;
        } finally {
            //do in any case a unlock
            unlockManage();
        }

        return ret;
    }

}
