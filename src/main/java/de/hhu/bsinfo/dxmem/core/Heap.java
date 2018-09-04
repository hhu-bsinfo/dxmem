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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Very efficient memory allocator for many small objects
 *
 * @author Florian Klein, florian.klein@hhu.de, 13.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 * @author Florian Hucke, florian.hucke@hhu.de, 06.02.2018
 */
public final class Heap implements Importable, Exportable {
    private static final Logger LOGGER = LogManager.getFormatterLogger(Heap.class.getSimpleName());

    static final int SIZE_MARKER_BYTE = 1;
    private static final byte ALLOC_BLOCK_FLAGS_OFFSET = 0x4;
    private static final long MAX_SET_SIZE = (long) Math.pow(2, 30);
    private static final byte SMALL_BLOCK_SIZE = 64;
    static final byte SINGLE_BYTE_MARKER = 0xF;
    static final byte HEAP_BORDER_MARKER = 0xE;
    static final byte UNTRACKED_FREE_BLOCK_MARKER = 0x1;
    // tracking free blocks with either 1 byte or 6 byte length field
    private static final byte TRACKED_FREE_BLOCK_FLAGS_OFFSET = 0x2;
    // minimum size for a free block to track: 2x length field (min. 1 byte each) after left marker and before
    // right marker + pointer to prev linked block and pointer to next linked block
    private static final int UNTRACKED_FREE_BLOCK_SIZE = 2 * Address.POINTER_SIZE + 2;

    private final VirtualMemoryBlock m_memory = new VirtualMemoryBlock();
    private final HeapStatus m_status = new HeapStatus();

    private long m_baseFreeBlockList;
    private int m_freeBlocksListSize = -1;
    private long[] m_freeBlockListSizes;
    private int m_freeBlocksListCount = -1;

    // protect concurrent malloc and free calls
    private final Lock m_lock = new ReentrantLock(false);

    /**
     * Constructor for importing from file
     */
    Heap() {
        LOGGER.info("Created 'invalid' Heap for loading dump from file");
    }

    /**
     * Creates an instance of the heap
     *
     * @param p_size
     *         The size of the heap in bytes (must be at least 1 MB)
     */
    Heap(final long p_size) {
        if (p_size < 1024 * 1024) {
            throw new MemoryRuntimeException("Minimum heap size is 1 MB (size specified: " + p_size + ')');
        }

        m_status.m_totalSizeBytes = p_size;

        LOGGER.info("Creating Heap, size %d bytes", p_size);

        m_memory.allocate(p_size);

        // Reset the memory block to zero. Do it in rather small sets to avoid ZooKeeper time-out
        int sets = (int) (p_size / MAX_SET_SIZE);

        for (int i = 0; i < sets; i++) {
            m_memory.set(MAX_SET_SIZE * i, MAX_SET_SIZE, (byte) 0);
        }

        if (p_size % MAX_SET_SIZE != 0) {
            m_memory.set(MAX_SET_SIZE * sets, (int) (p_size - sets * MAX_SET_SIZE), (byte) 0);
        }

        // according to memory size, have a proper amount of free memory block lists
        // -2, because we don't need a free block list for the full memory
        // and the first size greater than the full memory size
        // detect highest bit using log2 to have proper memory sizes
        m_freeBlocksListCount = (int) (Math.log(p_size) / Math.log(2)) - 2;
        m_freeBlocksListSize = m_freeBlocksListCount * Address.POINTER_SIZE;
        m_baseFreeBlockList = m_status.m_totalSizeBytes - m_freeBlocksListSize;

        // Initializes the list sizes
        m_freeBlockListSizes = new long[m_freeBlocksListCount];

        m_freeBlockListSizes[0] = UNTRACKED_FREE_BLOCK_SIZE;
        m_freeBlockListSizes[1] = 24;
        m_freeBlockListSizes[2] = 36;
        m_freeBlockListSizes[3] = 48;

        for (int i = 4; i < m_freeBlocksListCount; i++) {
            // 64, 128, ...
            m_freeBlockListSizes[i] = (long) Math.pow(2, i + 2);
        }

        LOGGER.debug("Created free block lists, m_freeBlocksListCount %d, m_freeBlocksListSize %d, " +
                " m_baseFreeBlockList %d", m_freeBlocksListCount, m_freeBlocksListSize, m_baseFreeBlockList);

        // Create one big free block
        // -2 for the marker bytes
        m_status.m_freeSizeBytes = m_status.m_totalSizeBytes - m_freeBlocksListSize - SIZE_MARKER_BYTE * 2;

        // mark start and end of heap area
        writeLeftPartOfMarker(0, HEAP_BORDER_MARKER);
        writeRightPartOfMarker(SIZE_MARKER_BYTE + m_status.m_freeSizeBytes, HEAP_BORDER_MARKER);

        // make area a free block
        createFreeBlock(SIZE_MARKER_BYTE, m_status.m_freeSizeBytes);

        m_status.m_freeBlocks = 1;
        m_status.m_freeSmall64ByteBlocks = 0;
    }

    /**
     * Free all memory of the heap
     */
    public void destroy() {
        m_memory.free();
    }

    /**
     * Gets the status of the heap
     *
     * @return the status
     */
    public HeapStatus getStatus() {
        return m_status;
    }

    /**
     * Allocate a block of memory
     *
     * @param p_size
     *         Payload size to allocate
     * @param p_entry
     *         Table entry object to write address and size of allocation to. Note: The entry value is not
     *         written back to the table. This must be handled by the caller
     * @return False if it can't find a block with the specified size which might mean that we are out of memory.
     * However, allocating smaller object sizes may still succeed.
     */
    public boolean malloc(final int p_size, final CIDTableChunkEntry p_entry) {
        return malloc(p_size, p_entry, false);
    }

    /**
     * Allocate a block of memory
     *
     * @param p_size
     *         Payload size to allocate
     * @param p_entry
     *         Table entry object to write address and size of allocation to. Note: The entry value is not
     *         written back to the table. This must be handled by the caller
     * @param p_noLengthField
     *         Even if a split length field is set, don't write any length information to the chunk to save memory.
     *         Used for (internal) static structures with known size (e.g. NID/LID tables)
     * @return False if it can't find a block with the specified size which might mean that we are out of memory.
     * However, allocating smaller object sizes may still succeed.
     */
    public boolean malloc(final int p_size, final CIDTableChunkEntry p_entry, final boolean p_noLengthField) {
        assert p_size > 0;
        assert p_entry != null;

        m_lock.lock();
        boolean ret = reserveBlock(p_size, p_entry, p_noLengthField);
        m_lock.unlock();

        return ret;
    }

    /**
     * Allocate multiple blocks of memory of the same size
     *
     * @param p_size
     *         Payload size to allocate
     * @param p_count
     *         Number of memory blocks of the specified payload size to allocate
     * @param p_entry
     *         Table entry object to write address and size of allocation to. Note: The entry value is not
     *         written back to the table. This must be handled by the caller
     * @return The number of memory locations allocated. On success, this should match the request value p_count.
     * If less than p_count, we might be out of memory. However, allocating smaller object sizes may still succeed.
     */
    public int malloc(final int p_size, final int p_count, final CIDTableChunkEntry[] p_entry) {
        assert p_size > 0;
        assert p_entry != null;
        assert p_count > 0;
        assert p_entry.length >= p_count;

        int successfulAllocs = 0;

        m_lock.lock();

        if (!multiReserveBlocks(p_size, p_count, p_entry)) {
            // large batch allocation failed, fallback to single malloc calls on failure

            for (int i = 0; i < p_count; i++) {
                if (!malloc(p_size, p_entry[i])) {
                    break;
                }

                successfulAllocs++;
            }
        } else {
            successfulAllocs = p_count;
        }

        m_lock.unlock();

        return successfulAllocs;
    }

    /**
     * Allocate multiple blocks of memory of different sizes
     *
     * @param p_entry
     *         Table entry object to write address and size of allocation to. Note: The entry value is not
     *         written back to the table. This must be handled by the caller
     * @param p_sizes
     *         Payload sizes to allocate separate memory blocks for
     * @return The number of memory locations allocated. On success, this should match the request value p_count.
     * If less than p_count, we might be out of memory. However, allocating smaller object sizes may still succeed.
     */
    public int malloc(final CIDTableChunkEntry[] p_entry, final int... p_sizes) {
        return malloc(p_entry, p_sizes, 0, p_sizes.length);
    }

    /**
     * Allocate multiple blocks of memory of different sizes
     *
     * @param p_entry
     *         Table entry object to write address and size of allocation to. Note: The entry value is not
     *         written back to the table. This must be handled by the caller
     * @param p_sizes
     *         Payload sizes to allocate separate memory blocks for
     * @param p_sizesOffset
     *         Start offset into sizes array
     * @param p_sizesLength
     *         Number of elements to consider of sizes array at specified start offset
     * @return The number of memory locations allocated. On success, this should match the request value p_count.
     * If less than p_count, we might be out of memory. However, allocating smaller object sizes may still succeed.
     */
    public int malloc(final CIDTableChunkEntry[] p_entry, final int[] p_sizes, final int p_sizesOffset,
            final int p_sizesLength) {
        assert p_entry != null;
        assert p_entry.length >= p_sizesLength;
        assert p_sizesOffset >= 0;
        assert p_sizesLength >= 0;

        int successfulAllocs = 0;

        m_lock.lock();

        if (!multiReserveBlocks(p_entry, p_sizes, p_sizesOffset, p_sizesLength)) {
            // large batch allocation failed, fallback to single malloc calls on failure

            for (int i = 0; i < p_sizes.length; i++) {
                if (!malloc(p_sizes[p_sizesOffset + i], p_entry[i])) {
                    break;
                }

                successfulAllocs++;
            }
        } else {
            successfulAllocs = p_sizes.length;
        }

        m_lock.unlock();

        return successfulAllocs;
    }

    /**
     * Free a memory block
     *
     * @param p_tableEntry
     *         CIDTable entry of the memory to free
     */
    public void free(final CIDTableChunkEntry p_tableEntry) {
        int payloadSize;

        // calculate total size of data block by merging the embedded length field and the the one stored in the block
        if (p_tableEntry.isLengthFieldEmbedded()) {
            payloadSize = p_tableEntry.getEmbeddedLengthField();
        } else {
            payloadSize = p_tableEntry.combineWithSplitLengthFieldData(
                    (int) read(p_tableEntry.getAddress() - p_tableEntry.getSplitLengthFieldSize(),
                            p_tableEntry.getSplitLengthFieldSize()));
        }

        m_lock.lock();

        // start address between marker and length field
        freeReservedBlock(p_tableEntry.getAddress() - p_tableEntry.getSplitLengthFieldSize(),
                p_tableEntry.getSplitLengthFieldSize(), payloadSize);

        m_lock.unlock();
    }

    /**
     * Resize an existing chunk
     *
     * @param p_tableEntry
     *         Table entry of existing chunk to resize
     * @param p_newSize
     *         New size
     */
    public boolean resize(final CIDTableChunkEntry p_tableEntry, final int p_newSize) {
        int oldSize = getSize(p_tableEntry);

        // don't resize if size did not change
        if (oldSize == p_newSize) {
            return true;
        }

        CIDTableChunkEntry[] newLocation = new CIDTableChunkEntry[1];
        newLocation[0] = new CIDTableChunkEntry();

        m_lock.lock();

        if (!multiReserveBlocks(newLocation, new int[] {p_newSize}, 0, 1)) {
            return false;
        }

        copyNative(newLocation[0].getAddress(), 0, p_tableEntry.getAddress(), 0, oldSize);

        // start address between marker and length field
        freeReservedBlock(p_tableEntry.getAddress() - p_tableEntry.getSplitLengthFieldSize(),
                p_tableEntry.getSplitLengthFieldSize(), oldSize);

        m_lock.unlock();

        p_tableEntry.setLengthField(p_newSize);
        p_tableEntry.setAddress(newLocation[0].getAddress());

        return true;
    }

    /**
     * Determine the actual chunk size using a valid cid table entry. Reads any data of a split length field
     * from the heap (if length field is split)
     *
     * @param p_tableEntry
     *         Valid cid table entry previously read from cid table
     * @return Payload size of the chunk (if split, the MSB split part will be stored in the p_tableEntry object, too)
     */
    public int getSize(final CIDTableChunkEntry p_tableEntry) {
        int payloadSize;

        // calculate total size of data block by merging the embedded length field and the the one stored in the block
        if (p_tableEntry.isLengthFieldEmbedded()) {
            payloadSize = p_tableEntry.getEmbeddedLengthField();
        } else {
            payloadSize = p_tableEntry.combineWithSplitLengthFieldData(
                    (int) read(p_tableEntry.getAddress() - p_tableEntry.getSplitLengthFieldSize(),
                            p_tableEntry.getSplitLengthFieldSize()));
        }

        return payloadSize;
    }

    /**
     * Overwrites the bytes in the memory with the given value
     *
     * @param p_address
     *         the address to start
     * @param p_size
     *         the number of bytes to overwrite
     * @param p_value
     *         the value to write
     */
    public void set(final long p_address, final long p_offset, final long p_size, final byte p_value) {
        assert assertMemoryBounds(p_address, p_offset, p_size);

        m_memory.set(p_address + p_offset, p_size, p_value);
    }

    /**
     * Copy from a source native memory region to the chunk (native) memory region
     *
     * @param p_address
     *         Address of the chunk
     * @param p_addressOffset
     *         Offset in the chunk to copy to
     * @param p_addressSource
     *         Native memory address of the data to copy
     * @param p_offset
     *         Offset to start in the source data
     * @param p_length
     *         Number of bytes to copy from the source
     */
    public void copyNative(final long p_address, final int p_addressOffset, final long p_addressSource,
            final int p_offset, final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length);

        m_memory.copyNative(p_address, p_addressOffset, p_addressSource, p_offset, p_length);
    }

    /**
     * Read a single byte from the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @return Byte read.
     */
    public byte readByte(final long p_address, final long p_offset) {
        assert assertMemoryBounds(p_address, p_offset, Byte.BYTES);

        return m_memory.readByte(p_address + p_offset);
    }

    /**
     * Read a single short from the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @return Short read.
     */
    public short readShort(final long p_address, final long p_offset) {
        assert assertMemoryBounds(p_address, p_offset, Short.BYTES);

        return m_memory.readShort(p_address + p_offset);
    }

    /**
     * Read a single char from the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @return Char read.
     */
    public char readChar(final long p_address, final long p_offset) {
        assert assertMemoryBounds(p_address, p_offset, Character.BYTES);

        return m_memory.readChar(p_address + p_offset);
    }

    /**
     * Read a single int from the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @return Int read.
     */
    public int readInt(final long p_address, final long p_offset) {
        assert assertMemoryBounds(p_address, p_offset, Integer.BYTES);

        return m_memory.readInt(p_address + p_offset);
    }

    /**
     * Read a long from the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @return Long read.
     */
    public long readLong(final long p_address, final long p_offset) {
        assert assertMemoryBounds(p_address, p_offset, Long.BYTES);

        return m_memory.readLong(p_address + p_offset);
    }

    /**
     * Read data into a byte array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to start address.
     * @param p_buffer
     *         Buffer to read into.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements read.
     */
    public int readBytes(final long p_address, final long p_offset, final byte[] p_buffer, final int p_offsetArray,
            final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length * Byte.BYTES);

        return m_memory.readBytes(p_address + p_offset, p_buffer, p_offsetArray, p_length);
    }

    /**
     * Read data into a short array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to start address.
     * @param p_buffer
     *         Buffer to read into.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements read.
     */
    public int readShorts(final long p_address, final long p_offset, final short[] p_buffer, final int p_offsetArray,
            final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length * Short.BYTES);

        return m_memory.readShorts(p_address + p_offset, p_buffer, p_offsetArray, p_length);
    }

    /**
     * Read data into a short array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to start address.
     * @param p_buffer
     *         Buffer to read into.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements read.
     */
    public int readChars(final long p_address, final long p_offset, final char[] p_buffer, final int p_offsetArray,
            final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length * Character.BYTES);

        return m_memory.readChars(p_address + p_offset, p_buffer, p_offsetArray, p_length);
    }

    /**
     * Read data into an int array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to start address.
     * @param p_buffer
     *         Buffer to read into.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements read.
     */
    public int readInts(final long p_address, final long p_offset, final int[] p_buffer, final int p_offsetArray,
            final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length * Integer.BYTES);

        return m_memory.readInts(p_address + p_offset, p_buffer, p_offsetArray, p_length);
    }

    /**
     * Read data into a long array.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to start address.
     * @param p_buffer
     *         Buffer to read into.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements read.
     */
    public int readLongs(final long p_address, final long p_offset, final long[] p_buffer, final int p_offsetArray,
            final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length * Long.BYTES);

        return m_memory.readLongs(p_address + p_offset, p_buffer, p_offsetArray, p_length);
    }

    /**
     * Write a single byte to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Byte to write.
     */
    public void writeByte(final long p_address, final long p_offset, final byte p_value) {
        assert assertMemoryBounds(p_address, p_offset, Byte.BYTES);

        m_memory.writeByte(p_address + p_offset, p_value);
    }

    /**
     * Write a short to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Short to write.
     */
    public void writeShort(final long p_address, final long p_offset, final short p_value) {
        assert assertMemoryBounds(p_address, p_offset, Short.BYTES);

        m_memory.writeShort(p_address + p_offset, p_value);
    }

    /**
     * Write a char to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Char to write.
     */
    public void writeChar(final long p_address, final long p_offset, final char p_value) {
        assert assertMemoryBounds(p_address, p_offset, Character.BYTES);

        m_memory.writeChar(p_address + p_offset, p_value);
    }

    /**
     * Write a single int to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Int to write.
     */
    public void writeInt(final long p_address, final long p_offset, final int p_value) {
        assert assertMemoryBounds(p_address, p_offset, Integer.BYTES);

        m_memory.writeInt(p_address + p_offset, p_value);
    }

    /**
     * Write a long value to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Long to write.
     */
    public void writeLong(final long p_address, final long p_offset, final long p_value) {
        assert assertMemoryBounds(p_address, p_offset, Long.BYTES);

        m_memory.writeLong(p_address + p_offset, p_value);
    }

    /**
     * Write an array of bytes to the specified address + offset.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Bytes to write.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements written.
     */
    public int writeBytes(final long p_address, final long p_offset, final byte[] p_value, final int p_offsetArray,
            final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length * Byte.BYTES);

        return m_memory.writeBytes(p_address + p_offset, p_value, p_offsetArray, p_length);
    }

    /**
     * Write an array of shorts to the heap.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Shorts to write.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements written.
     */
    public int writeShorts(final long p_address, final long p_offset, final short[] p_value, final int p_offsetArray,
            final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length * Short.BYTES);

        return m_memory.writeShorts(p_address + p_offset, p_value, p_offsetArray, p_length);
    }

    /**
     * Write an array of chars to the heap.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Chars to write.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements written.
     */
    public int writeChars(final long p_address, final long p_offset, final char[] p_value, final int p_offsetArray,
            final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length * Character.BYTES);

        return m_memory.writeChars(p_address + p_offset, p_value, p_offsetArray, p_length);
    }

    /**
     * Write an array of ints to the heap.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Ints to write.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements written.
     */
    public int writeInts(final long p_address, final long p_offset, final int[] p_value, final int p_offsetArray,
            final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length * Integer.BYTES);

        return m_memory.writeInts(p_address + p_offset, p_value, p_offsetArray, p_length);
    }

    /**
     * Write an array of longs to the heap.
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_value
     *         Longs to write.
     * @param p_offsetArray
     *         Offset within the buffer.
     * @param p_length
     *         Number of elements to read.
     * @return Number of elements written.
     */
    public int writeLongs(final long p_address, final long p_offset, final long[] p_value, final int p_offsetArray,
            final int p_length) {
        assert assertMemoryBounds(p_address, p_offset, p_length * Long.BYTES);

        return m_memory.writeLongs(p_address + p_offset, p_value, p_offsetArray, p_length);
    }

    /**
     * Atomic CAS operation on a long value
     *
     * @param p_address
     *         (Start) address of allocated memory block (taken from table entry)
     * @param p_offset
     *         Offset to add to the address.
     * @param p_expectedData
     *         Value expected at the specified location
     * @param p_newData
     *         New value to swap if the specified location matches p_expectedData
     * @return True if successful, false if the expected value is different to p_expectedData
     */
    public boolean casLong(final long p_address, final long p_offset, final long p_expectedData,
            final long p_newData) {
        assert assertMemoryBounds(p_address, p_offset, Long.BYTES);

        return m_memory.compareAndSwapLong(p_address + p_offset, p_expectedData, p_newData);
    }

    @Override
    public String toString() {
        return "Heap: " + m_status + ", m_baseFreeBlockList " + Address.toHexString(m_baseFreeBlockList) +
                ", m_freeBlocksListSize " + m_freeBlocksListSize + ", m_freeBlockListSizes " +
                Arrays.toString(m_freeBlockListSizes) + ", m_freeBlocksListCount " + m_freeBlocksListCount;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.exportObject(m_status);

        p_exporter.writeLong(m_baseFreeBlockList);
        p_exporter.writeInt(m_freeBlocksListSize);
        p_exporter.writeLongArray(m_freeBlockListSizes);
        p_exporter.writeInt(m_freeBlocksListCount);

        // separate metadata from VMB with padding
        p_exporter.writeLong(0xBBBBBBBBBBBBBBBBL);

        // write "chunks" of the raw memory to speed up the process
        byte[] buffer = new byte[1024 * 32];

        int chunkSize = buffer.length;
        long ptr = 0;

        while (ptr < m_status.getTotalSizeBytes()) {
            if (m_status.getTotalSizeBytes() - ptr < chunkSize) {
                chunkSize = (int) (m_status.getTotalSizeBytes() - ptr);
            }

            m_memory.readBytes(ptr, buffer, 0, chunkSize);
            p_exporter.writeBytes(buffer, 0, chunkSize);

            ptr += chunkSize;
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.importObject(m_status);

        m_baseFreeBlockList = p_importer.readLong(m_baseFreeBlockList);
        m_freeBlocksListSize = p_importer.readInt(m_freeBlocksListSize);
        m_freeBlockListSizes = p_importer.readLongArray(m_freeBlockListSizes);
        m_freeBlocksListCount = p_importer.readInt(m_freeBlocksListCount);

        // get rid of padding separating metadata from VMB
        p_importer.readLong(0);

        // free previously allocated VMB
        if (m_memory.isAllocated()) {
            m_memory.free();
        }

        // allocate VMB
        m_memory.allocate(m_status.getTotalSizeBytes());

        // read "chunks" from file and write to raw memory to speed up the process
        byte[] buffer = new byte[1024 * 32];

        int chunkSize = buffer.length;
        long ptr = 0;

        while (ptr < m_status.getTotalSizeBytes()) {
            if (m_status.getTotalSizeBytes() - ptr < chunkSize) {
                chunkSize = (int) (m_status.getTotalSizeBytes() - ptr);
            }

            p_importer.readBytes(buffer, 0, chunkSize);
            m_memory.writeBytes(ptr, buffer, 0, chunkSize);
            ptr += chunkSize;
        }
    }

    @Override
    public int sizeofObject() {
        throw new UnsupportedOperationException("Heap can be > 2 GB not fitting int type");
    }

    /**
     * For heap analysis and debugging, scan a chunk entry
     *
     * @param p_entry
     *         Entry to scan
     * @return HeapArea describing the memory area used by the specified entry
     */
    HeapArea scanChunkEntry(final CIDTableChunkEntry p_entry) {
        int chunkSize;

        if (p_entry.isLengthFieldEmbedded()) {
            chunkSize = p_entry.getEmbeddedLengthField();
        } else {
            chunkSize = p_entry.combineWithSplitLengthFieldData(
                    (int) read(p_entry.getAddress() - p_entry.getSplitLengthFieldSize(),
                            p_entry.getSplitLengthFieldSize()));
        }

        // heap areas start with front marker (including) and end with end marker (excluding)
        return new HeapArea(p_entry.getAddress() - p_entry.getSplitLengthFieldSize() - 1,
                p_entry.getAddress() + chunkSize);
    }

    /**
     * For heap analysis and debugging. Scan the free block lists of the heap
     *
     * @return List of memory areas that describe the free tracked blocks of the heap
     */
    ArrayList<HeapArea> scanFreeBlockLists() {
        long address;
        long freeSize;
        int freeLengthFieldSize;
        ArrayList<HeapArea> results = new ArrayList<>();

        for (int i = 0; i < m_freeBlocksListCount; i++) {
            address = readPointer(m_baseFreeBlockList + i * Address.POINTER_SIZE);

            // walk free block list
            while (address != Address.INVALID) {
                freeLengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - 1));
                freeSize = read(address, freeLengthFieldSize);

                results.add(new HeapArea(address - 1, address + freeSize));

                // continue with next pointer
                address = readPointer(address + freeLengthFieldSize + Address.POINTER_SIZE);
            }
        }

        return results;
    }

    /**
     * Read the right part of a marker byte
     *
     * @param p_address
     *         the address
     * @return the right part of a marker byte
     */
    int readRightPartOfMarker(final long p_address) {
        int marker = m_memory.readByte(p_address) & 0xF;
        assert assertMarker(marker, p_address);
        return marker;
    }

    /**
     * Read the left part of a marker byte
     *
     * @param p_address
     *         the address
     * @return the left part of a marker byte
     */
    private int readLeftPartOfMarker(final long p_address) {
        int marker = (m_memory.readByte(p_address) & 0xF0) >> 4;
        assert assertMarker(marker, p_address);
        return marker;
    }

    /**
     * Reads up to 8 bytes combined in a long
     *
     * @param p_address
     *         the address
     * @param p_count
     *         the number of bytes
     * @return the combined bytes
     */
    private long read(final long p_address, final int p_count) {
        return m_memory.readVal(p_address, p_count);
    }

    /**
     * Reads a pointer
     *
     * @param p_address
     *         the address
     * @return the pointer
     */
    private long readPointer(final long p_address) {
        return read(p_address, Address.POINTER_SIZE);
    }

    /**
     * Reserve a free block of memory.
     *
     * @param p_size
     *         Size of the block (payload size).
     * @param p_entry
     *         Table entry object to write address and size of allocation to. Note: The entry value is not
     *         written back to the table. This must be handled by the caller
     * @param p_noLengthField
     *         Even if a split length field is set, don't write any length information to the chunk to save memory.
     *         Used for (internal) static structures with known size (e.g. NID/LID tables)
     * @return False if it can't find a block with the specified size which might mean that we are out of memory.
     * However, allocating smaller object sizes may still succeed.
     */
    private boolean reserveBlock(final int p_size, final CIDTableChunkEntry p_entry, final boolean p_noLengthField) {
        assert p_size > 0;

        long address;
        int blockSize;
        int lengthFieldSplitSize;
        byte blockMarker;
        long lengthSplitMsb;

        p_entry.setLengthField(p_size);

        if (!p_entry.isLengthFieldEmbedded()) {
            lengthFieldSplitSize = p_entry.getSplitLengthFieldSize();
            lengthSplitMsb = p_entry.getSplitLengthFieldMsb();
        } else {
            lengthFieldSplitSize = 0;
            lengthSplitMsb = 0;
        }

        // omit length field for static and known structures like cid tables
        if (p_noLengthField) {
            lengthFieldSplitSize = 0;
        }

        blockMarker = (byte) (ALLOC_BLOCK_FLAGS_OFFSET + lengthFieldSplitSize);
        blockSize = p_size + lengthFieldSplitSize;
        address = findFreeBlock(blockSize);

        if (address != Address.INVALID) {
            unhookFreeBlock(address);
            trimFreeBlockToSize(address, blockSize);

            // Write marker
            writeLeftPartOfMarker(address + blockSize, blockMarker);
            writeRightPartOfMarker(address - SIZE_MARKER_BYTE, blockMarker);

            // Write split length field size if available
            if (lengthFieldSplitSize != 0) {
                write(address, lengthSplitMsb, lengthFieldSplitSize);
            }

            m_status.m_allocatedPayloadBytes += p_size;
            m_status.m_allocatedBlocks++;

            // address for user starts right at the payload
            p_entry.setAddress(address + lengthFieldSplitSize);

            return true;
        } else {
            return false;
        }
    }

    /**
     * Find a free block with a minimum size
     *
     * @param p_size
     *         Number of bytes that have to fit into that block
     * @return Address of the still hooked but free block
     */
    private long findFreeBlock(final int p_size) {
        int list;
        long address;
        long freeSize;
        int freeLengthFieldSize;

        // Get the list with a free block which is big enough
        list = getList(p_size) + 1;

        while (list < m_freeBlocksListCount && readPointer(m_baseFreeBlockList + list * Address.POINTER_SIZE) == 0) {
            list++;
        }

        if (list < m_freeBlocksListCount) {
            // A list is found
            address = readPointer(m_baseFreeBlockList + list * Address.POINTER_SIZE);
        } else {
            // Traverse through the lower list
            list = getList(p_size);
            address = readPointer(m_baseFreeBlockList + list * Address.POINTER_SIZE);

            if (address != Address.INVALID) {
                freeLengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - 1));
                freeSize = read(address, freeLengthFieldSize);

                while (freeSize < p_size && address != Address.INVALID) {
                    address = readPointer(address + freeLengthFieldSize + Address.POINTER_SIZE);

                    if (address != Address.INVALID) {
                        freeLengthFieldSize = getSizeFromMarker(readRightPartOfMarker(address - 1));
                        freeSize = read(address, freeLengthFieldSize);
                    }
                }
            }
        }

        return address;
    }

    /**
     * Uses an unhooked block and trims it to the right size to exactly fit the
     * specified number of bytes. The unused space is hooked back as free space.
     *
     * @param p_address
     *         Address of the unhooked block to trim
     * @param p_size
     *         Size to trim the block to
     */
    private void trimFreeBlockToSize(final long p_address, final long p_size) {
        long freeSize;
        int freeLengthFieldSize;

        freeLengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));
        freeSize = read(p_address, freeLengthFieldSize);

        if (freeSize == p_size) {
            m_status.m_freeSizeBytes -= p_size;
            m_status.m_freeBlocks--;

            if (freeSize < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks--;
            }
        } else if (freeSize == p_size + 1) {
            // 1 Byte to big -> write two markers on the right
            writeRightPartOfMarker(p_address + p_size, SINGLE_BYTE_MARKER);
            writeLeftPartOfMarker(p_address + p_size + 1, SINGLE_BYTE_MARKER);

            // +1 for the marker byte added
            m_status.m_freeSizeBytes -= p_size + 1;
            m_status.m_freeBlocks--;

            if (freeSize + 1 < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks--;
            }
        } else {
            // Block is too big -> create a new free block with the remaining size
            createFreeBlock(p_address + p_size + 1, freeSize - p_size - 1);

            // +1 for the marker byte added
            m_status.m_freeSizeBytes -= p_size + 1;

            if (freeSize >= SMALL_BLOCK_SIZE && freeSize - p_size - 1 < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks++;
            }
        }
    }

    /**
     * Reserve multiple blocks with a single call reducing metadata processing overhead
     *
     * @param p_size
     *         Payload size to allocate
     * @param p_count
     *         Number of memory blocks of the specified payload size to allocate
     * @param p_entry
     *         Table entry object to write address and size of allocation to. Note: The entry value is not
     *         written back to the table. This must be handled by the caller
     * @return True if multi allocating all blocks was successful, false on allocation failure (not sufficient memory
     * or could not find a single huge block to execute multi allocation)
     */
    private boolean multiReserveBlocks(final int p_size, final int p_count, final CIDTableChunkEntry[] p_entry) {
        long address;
        int lengthFieldSize;
        byte blockMarker;

        lengthFieldSize = CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(p_size);

        // number of marker bytes to separate blocks
        // -1: one marker byte is already part of the free block
        int bigChunkSize = p_count - 1;

        bigChunkSize += p_size * p_count;
        bigChunkSize += lengthFieldSize * p_count;

        address = findFreeBlock(bigChunkSize);

        // no free block found
        if (address == Address.INVALID) {
            return false;
        }

        unhookFreeBlock(address);
        trimFreeBlockToSize(address, bigChunkSize);

        for (int i = 0; i < p_entry.length; i++) {
            blockMarker = (byte) (ALLOC_BLOCK_FLAGS_OFFSET + lengthFieldSize);

            writeRightPartOfMarker(address - SIZE_MARKER_BYTE, blockMarker);
            writeLeftPartOfMarker(address + lengthFieldSize + p_size, blockMarker);

            p_entry[i].setLengthField(p_size);

            if (!p_entry[i].isLengthFieldEmbedded()) {
                write(address, p_entry[i].getSplitLengthFieldMsb(), p_entry[i].getSplitLengthFieldSize());
            }

            // chunk address starts after length field (if available)
            p_entry[i].setAddress(address + lengthFieldSize);

            // +1 : right side marker byte
            address += lengthFieldSize + p_size + 1;

            // update full size
            m_status.m_allocatedPayloadBytes += p_size;
            m_status.m_allocatedBlocks++;
        }

        return true;
    }

    /**
     * Reserve multiple blocks with a single call reducing metadata processing overhead
     *
     * @param p_entry
     *         Table entry object to write address and size of allocation to. Note: The entry value is not
     *         written back to the table. This must be handled by the caller
     * @param p_sizes
     *         Payload sizes to allocate separate memory blocks for
     * @param p_sizesOffset
     *         Offset to start in sizes array
     * @param p_sizesLength
     *         Number of elements from size array to consider for allocation
     * @return True if multi allocating all blocks was successful, false on allocation failure (not sufficient memory
     * or could not find a single huge block to execute multi allocation)
     */
    private boolean multiReserveBlocks(final CIDTableChunkEntry[] p_entry, final int[] p_sizes, final int p_sizesOffset,
            final int p_sizesLength) {
        long address;
        byte blockMarker;

        // number of marker bytes to separate blocks
        // -1: one marker byte is already part of the free block
        int bigChunkSize = p_sizes.length - 1;

        for (int i = 0; i < p_sizes.length; i++) {
            bigChunkSize += p_sizes[p_sizesOffset + i];
            bigChunkSize += CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(p_sizes[p_sizesOffset + i]);
        }

        address = findFreeBlock(bigChunkSize);

        // no free block found
        if (address == Address.INVALID) {
            return false;
        }

        unhookFreeBlock(address);
        trimFreeBlockToSize(address, bigChunkSize);

        for (int i = 0; i < p_entry.length; i++) {
            int lengthFieldSize = CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(p_sizes[p_sizesOffset + i]);

            blockMarker = (byte) (ALLOC_BLOCK_FLAGS_OFFSET + lengthFieldSize);

            writeRightPartOfMarker(address - SIZE_MARKER_BYTE, blockMarker);
            writeLeftPartOfMarker(address + lengthFieldSize + p_sizes[p_sizesOffset + i], blockMarker);

            p_entry[i].setLengthField(p_sizes[p_sizesOffset + i]);

            if (!p_entry[i].isLengthFieldEmbedded()) {
                write(address, p_entry[i].getSplitLengthFieldMsb(), p_entry[i].getSplitLengthFieldSize());
            }

            // chunk address starts after length field (if available)
            p_entry[i].setAddress(address + lengthFieldSize);

            // +1 : right side marker byte
            address += lengthFieldSize + p_sizes[p_sizesOffset + i] + 1;

            // update full size
            m_status.m_allocatedPayloadBytes += p_sizes[p_sizesOffset + i];
            m_status.m_allocatedBlocks++;
        }

        return true;
    }

    /**
     * Free a reserved block of memory
     *
     * @param p_address
     *         Address of the block
     * @param p_lengthFieldSize
     *         Size of the length field
     * @param p_payloadSize
     *         Size of the block's payload
     */
    private void freeReservedBlock(final long p_address, final int p_lengthFieldSize, final long p_payloadSize) {
        long freeSize;
        long address;
        boolean leftFree;
        long leftSize;
        boolean rightFree;
        long rightSize;

        assert assertMemoryBounds(p_address);

        freeSize = p_lengthFieldSize + p_payloadSize;
        address = p_address;

        // only merge if left neighbor exists (beginning of memory area)
        if (address - SIZE_MARKER_BYTE > Address.INVALID) {
            // Read left part of the marker on the left
            int leftMarker = readLeftPartOfMarker(address - SIZE_MARKER_BYTE);
            int leftLengthFieldSize = getSizeFromMarker(leftMarker);
            leftFree = true;

            switch (leftMarker) {
                case 0:
                    throw new MemoryRuntimeException("Invalid marker state 0 at address " +
                            Address.toHexString(address - SIZE_MARKER_BYTE));

                case UNTRACKED_FREE_BLOCK_MARKER:
                    // Left neighbor block (< 14 byte) is free -> merge free blocks
                    // -1, length field size is 1
                    leftSize = read(address - SIZE_MARKER_BYTE - leftLengthFieldSize, leftLengthFieldSize);
                    // merge marker byte
                    leftSize += SIZE_MARKER_BYTE;
                    break;

                case TRACKED_FREE_BLOCK_FLAGS_OFFSET:
                case TRACKED_FREE_BLOCK_FLAGS_OFFSET + 1:
                    // Left neighbor block is free -> merge free blocks
                    leftSize = read(address - SIZE_MARKER_BYTE - leftLengthFieldSize, leftLengthFieldSize);
                    // skip leftSize and marker byte from address to get block offset
                    unhookFreeBlock(address - leftSize - SIZE_MARKER_BYTE);
                    // we also merge the marker byte
                    leftSize += SIZE_MARKER_BYTE;
                    break;

                case SINGLE_BYTE_MARKER:
                    // Left byte is free -> merge free blocks
                    leftSize = SIZE_MARKER_BYTE;
                    break;

                default:
                    leftSize = 0;
                    leftFree = false;
                    break;
            }
        } else {
            leftSize = 0;
            leftFree = false;
        }

        // update start address of free block and size
        address -= leftSize;
        freeSize += leftSize;

        // Only merge if right neighbor within valid area (not inside or past free blocks list)
        if (p_address + p_lengthFieldSize + p_payloadSize + SIZE_MARKER_BYTE < m_baseFreeBlockList) {

            // Read right part of the marker on the right
            int rightMarker = readRightPartOfMarker(p_address + p_lengthFieldSize + p_payloadSize);
            int rightLengthFieldSize = getSizeFromMarker(rightMarker);
            rightFree = true;

            switch (rightMarker) {
                case 0:
                    throw new MemoryRuntimeException("Invalid marker state 0 at address " +
                            Address.toHexString(address - SIZE_MARKER_BYTE));

                case UNTRACKED_FREE_BLOCK_MARKER:
                    // Right neighbor block (< 14 byte) is free -> merge free blocks
                    // + 1 to skip marker byte
                    rightSize = read(p_address + p_lengthFieldSize + p_payloadSize + SIZE_MARKER_BYTE,
                            rightLengthFieldSize);
                    // merge marker byte
                    rightSize += SIZE_MARKER_BYTE;
                    break;

                case TRACKED_FREE_BLOCK_FLAGS_OFFSET:
                case TRACKED_FREE_BLOCK_FLAGS_OFFSET + 1:
                    // Right neighbor block is free -> merge free blocks
                    // + 1 to skip marker byte
                    rightSize = read(p_address + p_lengthFieldSize + p_payloadSize + SIZE_MARKER_BYTE,
                            rightLengthFieldSize);
                    unhookFreeBlock(p_address + p_lengthFieldSize + p_payloadSize + SIZE_MARKER_BYTE);
                    // we also merge the marker byte
                    rightSize += SIZE_MARKER_BYTE;
                    break;

                case SINGLE_BYTE_MARKER:
                    // Right byte is free -> merge free blocks
                    rightSize = SIZE_MARKER_BYTE;
                    break;

                default:
                    rightSize = 0;
                    rightFree = false;
                    break;
            }
        } else {
            rightSize = 0;
            rightFree = false;
        }

        // update size of full free block
        freeSize += rightSize;

        // Create a free block
        createFreeBlock(address, freeSize);

        if (!leftFree && !rightFree) {
            m_status.m_freeSizeBytes += p_payloadSize + p_lengthFieldSize;
            m_status.m_freeBlocks++;

            if (p_payloadSize + p_lengthFieldSize < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks++;
            }
        } else if (leftFree && !rightFree) {
            m_status.m_freeSizeBytes += p_payloadSize + p_lengthFieldSize + SIZE_MARKER_BYTE;

            if (p_payloadSize + p_lengthFieldSize + leftSize >= SMALL_BLOCK_SIZE && leftSize < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks--;
            }
        } else if (!leftFree /*&& rightFree*/) {
            m_status.m_freeSizeBytes += p_payloadSize + p_lengthFieldSize + SIZE_MARKER_BYTE;

            if (p_payloadSize + p_lengthFieldSize + rightSize >= SMALL_BLOCK_SIZE && rightSize < SMALL_BLOCK_SIZE) {
                m_status.m_freeSmall64ByteBlocks--;
            }
            // leftFree && rightFree
        } else {
            // +2 for two marker bytes being merged
            m_status.m_freeSizeBytes += p_payloadSize + p_lengthFieldSize + 2 * SIZE_MARKER_BYTE;
            m_status.m_freeBlocks--;

            if (p_payloadSize + p_lengthFieldSize + leftSize + rightSize >= SMALL_BLOCK_SIZE) {
                if (rightSize < SMALL_BLOCK_SIZE && leftSize < SMALL_BLOCK_SIZE) {
                    m_status.m_freeSmall64ByteBlocks--;
                } else if (rightSize >= SMALL_BLOCK_SIZE && leftSize >= SMALL_BLOCK_SIZE) {
                    m_status.m_freeSmall64ByteBlocks++;
                }
            }
        }

        m_status.m_allocatedPayloadBytes -= p_payloadSize;
        m_status.m_allocatedBlocks--;
    }

    /**
     * Check the memory bounds with the specified address.
     *
     * @param p_address
     *         Address to check if within memory.
     * @return Dummy return for assert
     */
    private boolean assertMemoryBounds(final long p_address) {
        if (p_address < 0 || p_address > m_status.m_totalSizeBytes) {
            throw new MemoryRuntimeException("Address " + p_address + " is not within memory: " + this);
        }

        return true;
    }

    /**
     * Check the memory bounds with the specified start address and size.
     *
     * @param p_address
     *         Address to check if within bounds.
     * @param p_length
     *         Number of bytes starting at address.
     * @return Dummy return for assert
     */
    private boolean assertMemoryBounds(final long p_address, final long p_offset, final long p_length) {
        if (p_address < 0) {
            throw new MemoryRuntimeException("Address negative: " + Address.toHexString(p_address) + ", " + p_offset +
                    ", " + p_length);
        }

        if (p_address > m_status.m_totalSizeBytes) {
            throw new MemoryRuntimeException("Address exceeds memory bounds (" + m_status.m_totalSizeBytes + ": " +
                    Address.toHexString(p_address) + ", " + p_offset + ", " + p_length);
        }

        if (p_offset < 0) {
            throw new MemoryRuntimeException("Offset negative: " + Address.toHexString(p_address) + ", " + p_offset +
                    ", " + p_length);
        }

        if (p_length < 0) {
            throw new MemoryRuntimeException("Length negative: " + Address.toHexString(p_address) + ", " + p_offset +
                    ", " + p_length);
        }

        if (p_address + p_offset > m_status.m_totalSizeBytes) {
            throw new MemoryRuntimeException(
                    "Address + offset exceeds memory bounds (" + m_status.m_totalSizeBytes + ": " +
                            Address.toHexString(p_address) + ", " + p_offset + ", " + p_length);
        }

        if (p_address + p_offset + p_length > m_status.m_totalSizeBytes) {
            throw new MemoryRuntimeException(
                    "Address + offset + length exceeds memory bounds (" + m_status.m_totalSizeBytes +
                            ": " + Address.toHexString(p_address) + ", " + p_offset + ", " + p_length);
        }

        return true;
    }

    /**
     * Creates a free block
     *
     * @param p_address
     *         the address
     * @param p_size
     *         the size
     */
    private void createFreeBlock(final long p_address, final long p_size) {
        assert p_address != Address.INVALID;
        assert assertMemoryBounds(p_address);
        assert p_size > 0;

        long listOffset;
        int lengthFieldSize;
        int marker;
        long anchor;

        if (p_size < UNTRACKED_FREE_BLOCK_SIZE) {
            // If size too small -> the block will not be hook in the lists
            lengthFieldSize = 1;
            marker = UNTRACKED_FREE_BLOCK_MARKER;

            // e.g. |M|L|M|, |M|L|L|M| or |M|L|...P...|L|M|
        } else {
            lengthFieldSize = 1;
            marker = TRACKED_FREE_BLOCK_FLAGS_OFFSET;

            // Calculate the number of bytes for the length field
            // either a single byte for free blocks that can't fit a "full size" length
            // field for up to 6 bytes or 6 bytes
            if (p_size >> 8 != 0) {
                lengthFieldSize = 6;
                marker = TRACKED_FREE_BLOCK_FLAGS_OFFSET + 1;
            }

            // Get the corresponding list
            listOffset = m_baseFreeBlockList + getList(p_size) * Address.POINTER_SIZE;

            // Hook block in list
            anchor = readPointer(listOffset);

            // Write pointer to list and successor
            writePointer(p_address + lengthFieldSize, listOffset);
            writePointer(p_address + lengthFieldSize + Address.POINTER_SIZE, anchor);

            if (anchor != Address.INVALID) {
                // Write pointer of successor
                int tmpLfs = getSizeFromMarker(readRightPartOfMarker(anchor - SIZE_MARKER_BYTE));
                writePointer(anchor + tmpLfs, p_address);
            }

            // Write pointer of list
            writePointer(listOffset, p_address);
        }

        // Write length
        write(p_address, p_size, lengthFieldSize);
        write(p_address + p_size - lengthFieldSize, p_size, lengthFieldSize);

        // Write right and left marker
        writeRightPartOfMarker(p_address - SIZE_MARKER_BYTE, marker);
        writeLeftPartOfMarker(p_address + p_size, marker);
    }

    /**
     * Unhooks a free block
     *
     * @param p_address
     *         the address
     */
    private void unhookFreeBlock(final long p_address) {
        int lengthFieldSize;
        long prevPointer;
        long nextPointer;

        // Read size of length field
        lengthFieldSize = getSizeFromMarker(readRightPartOfMarker(p_address - SIZE_MARKER_BYTE));

        // Read pointers
        prevPointer = readPointer(p_address + lengthFieldSize);
        nextPointer = readPointer(p_address + lengthFieldSize + Address.POINTER_SIZE);

        if (prevPointer >= m_baseFreeBlockList) {
            // Write Pointer of list
            writePointer(prevPointer, nextPointer);
        } else {
            // Write Pointer of predecessor
            writePointer(prevPointer + lengthFieldSize + Address.POINTER_SIZE, nextPointer);
        }

        if (nextPointer != Address.INVALID) {
            // Write pointer of successor
            writePointer(nextPointer + lengthFieldSize, prevPointer);
        }
    }

    /**
     * Gets the suitable list for the given size
     *
     * @param p_size
     *         the size
     * @return Index of the suitable list
     */
    private int getList(final long p_size) {
        int ret = 0;

        while (ret + 1 < m_freeBlockListSizes.length && m_freeBlockListSizes[ret + 1] <= p_size) {
            ret++;
        }

        return ret;
    }

    /**
     * Writes a marker byte
     *
     * @param p_address
     *         the address
     * @param p_right
     *         the right part
     */
    private void writeRightPartOfMarker(final long p_address, final int p_right) {
        byte marker;

        marker = (byte) ((m_memory.readByte(p_address) & 0xF0) + (p_right & 0xF));
        m_memory.writeByte(p_address, marker);
    }

    /**
     * Writes a marker byte
     *
     * @param p_address
     *         the address
     * @param p_left
     *         the left part
     */
    private void writeLeftPartOfMarker(final long p_address, final int p_left) {
        byte marker;

        marker = (byte) (((p_left & 0xF) << 4) + (m_memory.readByte(p_address) & 0xF));
        m_memory.writeByte(p_address, marker);
    }

    /**
     * Writes a pointer
     *
     * @param p_address
     *         the address
     * @param p_pointer
     *         the pointer
     */
    private void writePointer(final long p_address, final long p_pointer) {
        write(p_address, p_pointer, Address.POINTER_SIZE);
    }

    /**
     * Writes up to 8 bytes combined in a long
     *
     * @param p_address
     *         the address
     * @param p_bytes
     *         the combined bytes
     * @param p_count
     *         the number of bytes
     */
    private void write(final long p_address, final long p_bytes, final int p_count) {
        m_memory.writeVal(p_address, p_bytes, p_count);
    }

    /**
     * Extract the size of the length field of the allocated or free area
     * from the marker byte.
     *
     * @param p_marker
     *         Marker byte.
     * @return Size of the length field of block with specified marker byte.
     */
    private static int getSizeFromMarker(final int p_marker) {
        int ret;

        if (p_marker == SINGLE_BYTE_MARKER) {
            ret = 0;
        } else if (p_marker == UNTRACKED_FREE_BLOCK_MARKER || p_marker == TRACKED_FREE_BLOCK_FLAGS_OFFSET) {
            ret = 1;
        } else if (p_marker == TRACKED_FREE_BLOCK_FLAGS_OFFSET + 1) {
            ret = 6;
        } else {
            ret = p_marker - ALLOC_BLOCK_FLAGS_OFFSET;
        }

        return ret;
    }

    /**
     * Verify if the marker's value is valid
     *
     * @param p_marker
     *         Marker value to verify
     * @param p_address
     *         Address of the marker
     * @return True if ok, exception on failure
     */
    private static boolean assertMarker(final int p_marker, final long p_address) {
        switch (p_marker) {
            case UNTRACKED_FREE_BLOCK_MARKER:
            case TRACKED_FREE_BLOCK_FLAGS_OFFSET:
            case TRACKED_FREE_BLOCK_FLAGS_OFFSET + 1:
            case ALLOC_BLOCK_FLAGS_OFFSET:
            case ALLOC_BLOCK_FLAGS_OFFSET + 1:
            case ALLOC_BLOCK_FLAGS_OFFSET + 2:
            case ALLOC_BLOCK_FLAGS_OFFSET + 3:
            case HEAP_BORDER_MARKER:
            case SINGLE_BYTE_MARKER:
                return true;

            default:
                throw new MemoryRuntimeException("Invalid left or right marker value " + p_marker + " at address " +
                        Address.toHexString(p_address));
        }
    }
}
