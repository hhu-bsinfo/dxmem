package de.hhu.bsinfo.dxmem.old;

import de.hhu.bsinfo.dxmem.core.CIDTable;
import de.hhu.bsinfo.dxmem.core.Heap;
import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Management of memory accesses to existing objects with known data type
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 28.02.18
 * @projectname dxmem-memory
 */
@SuppressWarnings("unused")
public class MemoryDirectAccess {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MemoryDirectAccess.class.getSimpleName());


    private final Heap heap;
    private final CIDTable cidTable;


    /**
     * Constructor
     *
     * @param memoryManager
     *          The central unit which manages all memory accesses
     *
     */
    MemoryDirectAccess(MemoryManager memoryManager) {
        heap = memoryManager.heap;
        cidTable = memoryManager.cidTable;
    }

    /**
     * Read a single byte from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public byte readByte(final long p_chunkID, final int p_offset) throws MemoryRuntimeException {
        try {
            long entry = cidTable.get(p_chunkID);
            if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                return heap.readByte(entry, p_offset);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
            throw e;
        }
    }

    /**
     * Read a single short from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public short readShort(final long p_chunkID, final int p_offset) throws MemoryRuntimeException {
        try {
            long entry = cidTable.get(p_chunkID);
            if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                return heap.readShort(entry, p_offset);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
            throw e;
        }
    }

    /**
     * Read a single int from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public int readInt(final long p_chunkID, final int p_offset) throws MemoryRuntimeException {
        try {
            long entry = cidTable.get(p_chunkID);
            if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                return heap.readInt(entry, p_offset);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
            throw e;
        }
    }

    /**
     * Read a single long from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public long readLong(final long p_chunkID, final int p_offset) throws MemoryRuntimeException {

        try {
            long entry = cidTable.get(p_chunkID);
            if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                return heap.readLong(entry, p_offset);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
            throw e;
        }
    }

    /**
     * Write a single byte to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeByte(final long p_chunkID, final int p_offset, final byte p_value) throws MemoryRuntimeException {
        try {
            long entry = cidTable.get(p_chunkID);
            if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                heap.writeByte(entry, p_offset, p_value);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
            throw e;
        }

        return true;
    }

    /**
     * Write a single short to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeShort(final long p_chunkID, final int p_offset, final short p_value) throws MemoryRuntimeException {
        try {
            long entry = cidTable.get(p_chunkID);
            if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                heap.writeShort(entry, p_offset, p_value);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
            throw e;
        }

        return true;
    }

    /**
     * Write a single int to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeInt(final long p_chunkID, final int p_offset, final int p_value) throws MemoryRuntimeException {
        try {
            long entry = cidTable.get(p_chunkID);
            if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                heap.writeInt(entry, p_offset, p_value);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
            throw e;
        }

        return true;
    }

    /**
     * Write a single long to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeLong(final long p_chunkID, final int p_offset, final long p_value) throws MemoryRuntimeException {
        try {
            long entry = cidTable.get(p_chunkID);
            if (entry != CIDTable.FREE_ENTRY && entry != CIDTable.ZOMBIE_ENTRY) {
                heap.writeLong(entry, p_offset, p_value);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(heap, e, ".", false, LOGGER);
            throw e;
        }

        return true;
    }
}
