package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.mem.exceptions.MemoryRuntimeException;
import de.hhu.bsinfo.dxram.mem.manipulation.ByteDataManipulation;
import de.hhu.bsinfo.dxram.mem.manipulation.ChunkDataManipulationTesting;
import de.hhu.bsinfo.dxram.mem.manipulation.DataStructureManipulation;
import de.hhu.bsinfo.dxutils.FastByteUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.*;

/**
 * Manage memory accesses to existing objects.
 *
 * All operations are Thread-Safe
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 28.02.18
 * @projectname dxram-memory
 */
@SuppressWarnings({"unused", "UnusedReturnValue"})
public class MemoryAccess {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MemoryAccess.class.getSimpleName());

    private final MemoryManager memory;
    private final SmallObjectHeap smallObjectHeap;
    private final CIDTable cidTable;

    /**
     * Constructor
     *
     * @param memoryManager
     *          The central unit which manages all memory accesses
     */
    MemoryAccess(final MemoryManager memoryManager) {
        memory = memoryManager;
        smallObjectHeap = memoryManager.smallObjectHeap;
        cidTable = memoryManager.cidTable;
    }

    //GET---------------------------------------------------------------------------------------------------------------

    /**
     * Get the payload of a chunk/data structure.
     * This operation is Thread-Safe
     *
     * @param p_dataStructure
     *          Data structure to read specified by its ID.
     * @return
     *          True if getting the chunk payload was successful, false if no chunk with the ID specified exists.
     */
    public boolean get(final DataStructure p_dataStructure) {
        long directEntryAddress;

        long entry;
        boolean ret = false;


        // #if LOGGER == TRACE
        LOGGER.trace("ENTER get p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        if (p_dataStructure.getID() == ChunkID.INVALID_ID) { //Check if CID can be correct
            p_dataStructure.setState(ChunkState.INVALID_ID);
        } else if ((directEntryAddress = cidTable.getAddressOfEntry(p_dataStructure.getID())) == SmallObjectHeap.INVALID_ADDRESS) { //Check if a CID exist
            p_dataStructure.setState(ChunkState.DOES_NOT_EXIST);
        } else if (memory.switchableReadLock(directEntryAddress)) { //check if a lock was received
            try {

                // #ifdef STATISTICS
                //->SOP_GET.enter();
                // #endif /* STATISTICS */

                entry = cidTable.directGet(directEntryAddress);
                if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                    assert smallObjectHeap.getSizeDataBlock(entry) == p_dataStructure.sizeofObject();

                    // pool the im/exporters
                    SmallObjectHeapDataStructureImExporter importer = memory.getImExporter(entry);
                    importer.importObject(p_dataStructure);

                    p_dataStructure.setState(ChunkState.OK);
                    ret = true;
                } else {
                    p_dataStructure.setState(ChunkState.DOES_NOT_EXIST);
                }

                // #ifdef STATISTICS
                //->SOP_GET.leave();
                // #endif /* STATISTICS */

            } catch (final MemoryRuntimeException e) {
                MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                throw e;
            } finally {
                memory.switchableReadUnlock(directEntryAddress);
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT get p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Get the payload of a chunk/data structure.
     * This operation is Thread-Safe
     *
     * @param p_dataStructure
     *          Data structure to read specified by its ID.
     * @param p_acquireWriteLock
     *          Acquire write lock to manipulate the data
     * @return
     *          True if getting the chunk payload was successful, false if no chunk with the ID specified exists.
     */
    public boolean get(final DataStructure p_dataStructure, final boolean p_acquireWriteLock) {
        long directEntryAddress;

        long entry;
        boolean ret = false;
        boolean gotLock;

        long chunkID = p_dataStructure.getID();

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER get p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */


        if (chunkID == ChunkID.INVALID_ID) { //Check if CID can be correct
            p_dataStructure.setState(ChunkState.INVALID_ID);
        } else if ((directEntryAddress = cidTable.getAddressOfEntry(chunkID)) != SmallObjectHeap.INVALID_ADDRESS ) { //Check if a CID exist
            p_dataStructure.setState(ChunkState.DOES_NOT_EXIST);
        } else {
            if (p_acquireWriteLock)
                gotLock = memory.switchableWriteLock(directEntryAddress);
            else
                gotLock = memory.switchableReadLock(directEntryAddress);


            if (gotLock) { //check if a lock was received
                try {
                    // #ifdef STATISTICS
                    //->SOP_GET.enter();
                    // #endif /* STATISTICS */

                    entry = cidTable.directGet(directEntryAddress);
                    if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                        assert smallObjectHeap.getSizeDataBlock(entry) == p_dataStructure.sizeofObject();

                        // pool the im/exporters
                        SmallObjectHeapDataStructureImExporter importer = memory.getImExporter(entry);
                        importer.importObject(p_dataStructure);

                        p_dataStructure.setState(ChunkState.OK);
                        ret = true;
                    } else {
                        p_dataStructure.setState(ChunkState.DOES_NOT_EXIST);
                    }

                    // #ifdef STATISTICS
                    //->SOP_GET.leave();
                    // #endif /* STATISTICS */

                } catch (final MemoryRuntimeException e) {
                    MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                    if (p_acquireWriteLock)
                        memory.switchableWriteUnlock(directEntryAddress);
                    throw e;
                } finally {
                    if (!p_acquireWriteLock)
                        memory.switchableReadUnlock(directEntryAddress);
                }
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT get p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Get the binary data of a chunk when the chunk size is unknown.
     * This method use a switchable lock //TODO Reset to normal lock
     *
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *          Read the chunk data of the specified ID
     * @return
     *          A byte array with payload if getting the chunk payload
     *          was successful, null if no chunk with the ID exists.
     */
    public byte[] get(final long p_chunkID) {
        long directEntryAddress;

        byte[] ret = null;
        long entry;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER get p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        if (p_chunkID != ChunkID.INVALID_ID && //Check if CID can be correct
                (directEntryAddress = cidTable.getAddressOfEntry(p_chunkID)) != SmallObjectHeap.INVALID_ADDRESS && //Check if a CID exist
                memory.switchableReadLock(directEntryAddress)) { //check if a lock was received
            try {

                // #ifdef STATISTICS
                //->SOP_GET.enter();
                // #endif /* STATISTICS */

                entry = cidTable.directGet(directEntryAddress);
                if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                    int chunkSize = smallObjectHeap.getSizeDataBlock(entry);
                    ret = new byte[chunkSize];

                    // pool the im/exporters
                    SmallObjectHeapDataStructureImExporter importer = memory.getImExporter(entry);
                    int retSize = importer.readBytes(ret);
                    if (retSize != chunkSize) {
                        //->throw new DXRAMRuntimeException("Unknown error, importer size " + retSize + " != chunk size " + chunkSize);
                        throw new RuntimeException("Unknown error, importer size " + retSize + " != chunk size " + chunkSize);//<<
                    }
                }

                // #ifdef STATISTICS
                //->SOP_GET.leave();
                // #endif /* STATISTICS */

            } catch (final MemoryRuntimeException e) {
                MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                throw e;
            } finally {
                memory.switchableReadUnlock(directEntryAddress);
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT get p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Get the binary data of a chunk when the chunk size is unknown.
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *          Read the chunk data of the specified ID
     * @param p_acquireWriteLock
     *          Acquire write lock to manipulate the data
     * @return
     *          A byte array with payload if getting the chunk payload
     *          was successful, null if no chunk with the ID exists.
     */
    public byte[] get(final long p_chunkID, final boolean p_acquireWriteLock) {
        long directEntryAddress;

        byte[] ret = null;
        long entry;
        boolean gotLock = false;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER get p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        if ((directEntryAddress = cidTable.getAddressOfEntry(p_chunkID)) != SmallObjectHeap.INVALID_ADDRESS) { //Check if a CID exist
            if (p_acquireWriteLock)
                gotLock = memory.switchableWriteLock(directEntryAddress);
            else
                gotLock = memory.switchableReadLock(directEntryAddress);
        }

        if (gotLock) { //check if a lock was received
            try {

                // #ifdef STATISTICS
                //->SOP_GET.enter();
                // #endif /* STATISTICS */

                entry = cidTable.directGet(directEntryAddress);
                if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {

                    //while weak consistency it is possible to work with a deleted chunk
                    if(memory.readLockDisabled() && ADDRESS.get(entry) >= smallObjectHeap.m_baseFreeBlockList){
                        memory.switchableReadUnlock(directEntryAddress);
                        return null;
                    }

                    int chunkSize = smallObjectHeap.getSizeDataBlock(entry);
                    ret = new byte[chunkSize];

                    // pool the im/exporters
                    SmallObjectHeapDataStructureImExporter importer = memory.getImExporter(entry);
                    int retSize = importer.readBytes(ret);
                    if (retSize != chunkSize) {
                        //->throw new DXRAMRuntimeException("Unknown error, importer size " + retSize + " != chunk size " + chunkSize);
                        throw new RuntimeException("Unknown error, importer size " + retSize + " != chunk size " + chunkSize);//<<
                    }
                }

                // #ifdef STATISTICS
                //->SOP_GET.leave();
                // #endif /* STATISTICS */

            } catch (final MemoryRuntimeException e) {
                if (p_acquireWriteLock)
                    memory.switchableWriteUnlock(directEntryAddress);

                MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                throw e;
            } finally {
                if (!p_acquireWriteLock)
                    memory.switchableReadUnlock(directEntryAddress);
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT get p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Get the binary data of a chunk when the chunk size is known
     *
     * @param p_chunkID
     *          Read the chunk data of the specified ID
     * @param p_buffer
     *          Buffer to save data
     * @param p_offset
     *          Chunk offset
     * @param p_bufferSize
     *          Size of the buffer
     * @param p_acquireWriteLock
     *          Acquire write lock to manipulate the data
     * @return the number of read bytes
     */
    //TODO testing
    public int get(final long p_chunkID, final byte[] p_buffer, final int p_offset, final int p_bufferSize, final boolean p_acquireWriteLock) {
        long directEntryAddress;

        int ret = -1;
        long entry;
        boolean gotLock;

        //Check if a CID exist
        if ((directEntryAddress = cidTable.getAddressOfEntry(p_chunkID)) != SmallObjectHeap.INVALID_ADDRESS) {
            return -1;
        }

        if (p_acquireWriteLock)
            gotLock = memory.switchableWriteLock(directEntryAddress);
        else
            gotLock = memory.switchableReadLock(directEntryAddress);

        //check if a lock was received
        if (gotLock) {
            try {
                // #ifdef STATISTICS
                //->SOP_GET.enter();
                // #endif /* STATISTICS */

                entry = cidTable.directGet(directEntryAddress);
                if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                    int chunkSize = smallObjectHeap.getSizeDataBlock(entry);

                    if (p_offset + chunkSize > p_bufferSize) {
                        ret = 0;
                    } else {
                        // pool the im/exporters
                        SmallObjectHeapDataStructureImExporter importer = memory.getImExporter(entry);
                        ret = importer.readBytes(p_buffer, p_offset, chunkSize);
                        if (ret != chunkSize) {
                            //->throw new DXRAMRuntimeException("Unknown error, importer size " + ret + " != chunk size " + chunkSize);
                            throw new RuntimeException("Unknown error, importer size " + ret + " != chunk size " + chunkSize);//<<
                        }
                    }
                }
                // #ifdef STATISTICS
                //->SOP_GET.leave();
                // #endif /* STATISTICS */
            } catch (final MemoryRuntimeException e) {
                if (p_acquireWriteLock)
                    memory.switchableWriteUnlock(directEntryAddress);
                MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                throw e;
            } finally {
                if (!p_acquireWriteLock)
                    memory.switchableReadUnlock(directEntryAddress);
            }
        }

        return ret;
    }

    //PUT---------------------------------------------------------------------------------------------------------------

    /**
     * Put data of the a data structure/chunk to the memory
     * This operation is Thread-Safe
     *
     * @param p_dataStructure
     *          Data structure to put
     * @return
     *          True if putting the data was successful, false if
     *          no chunk with the specified id exists
     */
    public boolean put(final DataStructure p_dataStructure) {
        return put(p_dataStructure, true);
    }

    /**
     * Put data of the a data structure/chunk to the memory
     * This operation is Thread-Safe
     *
     * @param p_dataStructure
     *          Data structure to put
     * @param p_acquireWriteLock
     *          Do we need a write lock or is a write lock set
     * @return
     *          True if putting the data was successful, false if
     *          no chunk with the specified id exists
     */
    public boolean put(final DataStructure p_dataStructure, final boolean p_acquireWriteLock) {
        long directEntryAddress;

        boolean ret = false;
        long entry;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER put p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        if (p_dataStructure.getID() == ChunkID.INVALID_ID) { //Check if CID can be correct
            p_dataStructure.setState(ChunkState.INVALID_ID);
        } else if ((directEntryAddress = cidTable.getAddressOfEntry(p_dataStructure.getID())) == SmallObjectHeap.INVALID_ADDRESS) { //Check if a CID exist
            p_dataStructure.setState(ChunkState.DOES_NOT_EXIST);
        } else if (!p_acquireWriteLock || memory.switchableWriteLock(directEntryAddress)) { //check if a lock was received
            try {
                // #ifdef STATISTICS
                //->SOP_PUT.enter();
                // #endif /* STATISTICS */

                entry = cidTable.directGet(directEntryAddress);
                if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                    assert smallObjectHeap.getSizeDataBlock(entry) == p_dataStructure.sizeofObject();

                    // pool the im/exporters
                    SmallObjectHeapDataStructureImExporter exporter = memory.getImExporter(entry);
                    exporter.exportObject(p_dataStructure);

                    p_dataStructure.setState(ChunkState.OK);

                    ret = true;
                } else {
                    p_dataStructure.setState(ChunkState.DOES_NOT_EXIST);
                }

                // #ifdef STATISTICS
                //->SOP_PUT.leave();
                // #endif /* STATISTICS */

            } catch (final MemoryRuntimeException e) {
                MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                throw e;
            } finally {
                memory.switchableWriteUnlock(directEntryAddress);
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT put p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Put some data into a chunk.
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *          Chunk ID for the data to put
     * @param p_data
     *          Chunk data to put
     * @return
     *          True if putting the data was successful, false if
     *          no chunk with the specified id exists
     */
    public boolean put(final long p_chunkID, final byte[] p_data) {
        return put(p_chunkID, p_data, true);
    }

    /**
     * Put some data into a chunk.
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *          Chunk ID for the data to put
     * @param p_data
     *          Chunk data to put
     * @param p_acquireWriteLock
     *          Do we need a write lock or is a write lock set
     * @return
     *          True if putting the data was successful, false if
     *          no chunk with the specified id exists
     */
    public boolean put(final long p_chunkID, final byte[] p_data, final boolean p_acquireWriteLock) {
        return put(p_chunkID, p_data, 0, p_data.length, p_acquireWriteLock);
    }

    /**
     * Put some data into a chunk.
     * This method use a switchable lock //TODO Reset to normal lock
     *
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *          Chunk ID for the data to put
     * @param p_data
     *          Chunk data to put
     * @param p_offset
     *          Offset for p_data array
     * @param p_length
     *          Number of bytes to put
     * @return
     *          True if putting the data was successful, false if
     *          no chunk with the specified id exists
     */
    boolean put(final long p_chunkID, final byte[] p_data, final int p_offset, final int p_length) {
        return put(p_chunkID, p_data, p_offset, p_length, true);
    }

    /**
     * Put some data into a chunk.
     * This method use a switchable lock //TODO Reset to normal lock
     *
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *          Chunk ID for the data to put
     * @param p_data
     *          Chunk data to put
     * @param p_offset
     *          Offset for p_data array
     * @param p_length
     *          Number of bytes to put
     * @param p_acquireWriteLock
     *          Do we need a write lock or is a write lock set
     * @return
     *          True if putting the data was successful, false if
     *          no chunk with the specified id exists
     */
    boolean put(final long p_chunkID, final byte[] p_data, final int p_offset, final int p_length, final boolean p_acquireWriteLock) {
        long directEntryAddress;

        long entry;
        boolean ret = false;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER put p_chunkID 0x%X, p_offset %d, p_length %d", p_chunkID, p_offset, p_length);
        // #endif /* LOGGER == TRACE */


        if (p_chunkID != ChunkID.INVALID_ID) { //Check if CID can be correct
            if ((directEntryAddress = cidTable.getAddressOfEntry(p_chunkID)) != SmallObjectHeap.INVALID_ADDRESS) { //Check if a CID exist
                if (!p_acquireWriteLock || memory.switchableWriteLock(directEntryAddress)) { //check if a lock was received
                    try {

                        // #ifdef STATISTICS
                        //->SOP_PUT.enter();
                        // #endif /* STATISTICS */

                        entry = cidTable.directGet(directEntryAddress);
                        if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                            assert p_offset + p_length <= smallObjectHeap.getSizeDataBlock(entry) : "offset: " + p_offset + "\tlength: " + p_length + "\tbs: " + smallObjectHeap.getSizeDataBlock(entry);

                            smallObjectHeap.writeBytes(entry, 0, p_data, p_offset, p_length);
                            ret = true;
                        }

                        // #ifdef STATISTICS
                        //->SOP_PUT.leave();
                        // #endif /* STATISTICS */
                    } catch (final MemoryRuntimeException e) {
                        MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                        throw e;
                    } finally {
                        memory.switchableWriteUnlock(directEntryAddress);
                    }
                }
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT put p_chunkID 0x%X, p_offset %d, p_length %d", p_chunkID, p_offset, p_length);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    //MODIFY------------------------------------------------------------------------------------------------------------

    /**
     * Modify some data in a chunk.
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *          Chunk ID for the data to put
     * @param byteDataManipulation
 *              A Interface to manipulate the data of the chunk
     * @return
     *          True if modifying the data was successful, false if
     *          no chunk with the specified id exists
     */
    boolean modify(final long p_chunkID, ByteDataManipulation byteDataManipulation) {
        long directEntryAddress;

        long entry;
        boolean ret = false;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER modify p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        if (p_chunkID != ChunkID.INVALID_ID && //Check if CID can be correct
                (directEntryAddress = cidTable.getAddressOfEntry(p_chunkID)) != SmallObjectHeap.INVALID_ADDRESS && //Check if a CID exist
                memory.switchableWriteLock(directEntryAddress)) { //check if a lock was received

            try {

                // #ifdef STATISTICS
                //->SOP_PUT.enter();
                // #endif /* STATISTICS */

                entry = cidTable.directGet(directEntryAddress);
                if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                    int fullSize = smallObjectHeap.getSizeDataBlock(entry);
                    byte[] data = new byte[fullSize];
                    smallObjectHeap.readBytes(entry, 0, data, 0, data.length);
                    smallObjectHeap.writeBytes(entry, 0, byteDataManipulation.getNewData(data), 0, data.length);
                    ret = true;
                }

                // #ifdef STATISTICS
                //->SOP_PUT.leave();
                // #endif /* STATISTICS */
            } catch (final MemoryRuntimeException e) {
                MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                throw e;
            } finally {
                memory.switchableWriteUnlock(directEntryAddress);
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT modify p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        return ret;
    }


    /**
     * Modify some data in a chunk.
     * This operation is Thread-Safe
     *
     * @param p_dataStructure
     *          DataStructure which hold the chunk
     * @param dataStructureManipulation
     *          FunctionalInterface to manipulate the data of the chunk
     * @return
     *          True if modifying the data was successful, false if
     *          no chunk with the specified id exists
     */
    boolean modify(final DataStructure p_dataStructure, DataStructureManipulation<DataStructure> dataStructureManipulation) {
        long directEntryAddress;

        boolean ret = false;
        long entry;
        long address;
        boolean deleted;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER put p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        if (p_dataStructure.getID() == ChunkID.INVALID_ID) { //Check if CID can be correct
            p_dataStructure.setState(ChunkState.INVALID_ID);
        } else if ((directEntryAddress = cidTable.getAddressOfEntry(p_dataStructure.getID())) == SmallObjectHeap.INVALID_ADDRESS) { //Check if a CID exist
            p_dataStructure.setState(ChunkState.DOES_NOT_EXIST);
        } else if (memory.switchableWriteLock(directEntryAddress)) { //check if a lock was received
            try {
                // #ifdef STATISTICS
                //->SOP_PUT.enter();
                // #endif /* STATISTICS */

                entry = cidTable.directGet(directEntryAddress);
                address = ADDRESS.get(entry);
                deleted = FULL_FLAG.get(entry);
                if (address > SmallObjectHeap.INVALID_ADDRESS && !deleted) {
                    assert smallObjectHeap.getSizeDataBlock(entry) == p_dataStructure.sizeofObject();

                    // pool the im/exporters
                    SmallObjectHeapDataStructureImExporter importer = memory.getImExporter(entry);
                    importer.importObject(p_dataStructure);

                    SmallObjectHeapDataStructureImExporter exporter = memory.getImExporter(entry);
                    exporter.exportObject(dataStructureManipulation.getNewData(p_dataStructure));

                    p_dataStructure.setState(ChunkState.OK);

                    ret = true;
                } else {
                    p_dataStructure.setState(ChunkState.DOES_NOT_EXIST);
                }

                // #ifdef STATISTICS
                //->SOP_PUT.leave();
                // #endif /* STATISTICS */

            } catch (final MemoryRuntimeException e) {
                MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                throw e;
            } finally {
                memory.switchableWriteUnlock(directEntryAddress);
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT put p_dataStructure 0x%X", p_dataStructure.getID());
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    //ONLY FOR TESTING--------------------------------------------------------------------------------------------------

    /**
     * Method for testing the MODIFY method. The only change is that an extended interface is used so that the
     * interface implementation can test data consistency.
     *
     * @param p_chunkID
     *          Chunk ID
     * @param chunkDataManipulation
     *          Implementation of the Interface
     * @param selected
     *          Index in a array for the used chunk
     * @return
     *          True if modifying the data was successful, false if
     *          no chunk with the specified id exists
     */
    @SuppressWarnings("UnusedReturnValue")
    boolean modifyTest(final long p_chunkID, ChunkDataManipulationTesting chunkDataManipulation, final int selected) {
        long directEntryAddress;

        long entry;
        boolean ret = false;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER modify p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        if (p_chunkID != ChunkID.INVALID_ID && //Check if CID can be correct
                (directEntryAddress = cidTable.getAddressOfEntry(p_chunkID)) != SmallObjectHeap.INVALID_ADDRESS && //Check if a CID exist
                memory.switchableWriteLock(directEntryAddress)) { //check if a lock was received

            try {

                // #ifdef STATISTICS
                //->SOP_PUT.enter();
                // #endif /* STATISTICS */

                entry = cidTable.directGet(directEntryAddress);
                if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                    int fullSize = smallObjectHeap.getSizeDataBlock(entry);
                    byte[] data = new byte[fullSize];
                    smallObjectHeap.readBytes(entry, 0, data, 0, data.length);
                    smallObjectHeap.writeBytes(entry, 0, chunkDataManipulation.getNewData(data, selected), 0, data.length);
                    ret = true;
                }

                // #ifdef STATISTICS
                //->SOP_PUT.leave();
                // #endif /* STATISTICS */
            } catch (final MemoryRuntimeException e) {
                MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                throw e;
            } finally {
                memory.switchableWriteUnlock(directEntryAddress);
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT modify p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Method for testing the get method.
     * The only change is that the method can test data consistency.
     *
     * @param p_chunkID
     *          Chunk ID
     * @param ref
     *          Array of longs which contain the expected value for every chunk
     * @param selected
     *          The index for this chunk
     * @return
     *          A byte array with payload if getting the chunk payload
     *          was successful, null if no chunk with the ID exists.
     */
    public byte[] getTesting(final long p_chunkID, final long[] ref, final int selected) {
        long directEntryAddress;

        byte[] ret = null;
        long entry;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER get p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        if (p_chunkID != ChunkID.INVALID_ID && //Check if CID can be correct
                (directEntryAddress = cidTable.getAddressOfEntry(p_chunkID)) != SmallObjectHeap.INVALID_ADDRESS && //Check if a CID exist
                memory.switchableReadLock(directEntryAddress)) { //check if a lock was received

            try {

                // #ifdef STATISTICS
                //->SOP_GET.enter();
                // #endif /* STATISTICS */

                entry = cidTable.directGet(directEntryAddress);
                if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                    int chunkSize = smallObjectHeap.getSizeDataBlock(entry);
                    ret = new byte[chunkSize];

                    // pool the im/exporters
                    SmallObjectHeapDataStructureImExporter importer = memory.getImExporter(entry);
                    int retSize = importer.readBytes(ret);

                    if (retSize != chunkSize) {
                        //->throw new DXRAMRuntimeException("Unknown error, importer size " + retSize + " != chunk size " + chunkSize);
                        throw new RuntimeException("Unknown error, importer size " + retSize + " != chunk size " + chunkSize);//<<
                    }

                    //START TESTING-----------------------------------------------------------------------------------
                    long tmp = FastByteUtils.bytesToLong(ret);
                    if (ref[selected] != tmp) {
                        LOGGER.error("read expected: " + ref[selected] + " get: " + tmp);
                        System.exit(2);
                    }
                    //END TESTING-----------------------------------------------------------------------------------
                }

                // #ifdef STATISTICS
                //->SOP_GET.leave();
                // #endif /* STATISTICS */

            } catch (final MemoryRuntimeException e) {
                MemoryError.handleMemDumpOnError(smallObjectHeap, e, ".", false, LOGGER);
                throw e;
            } finally {
                memory.switchableReadUnlock(directEntryAddress);
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT get p_chunkID 0x%X", p_chunkID);
        // #endif /* LOGGER == TRACE */

        return ret;
    }
}
