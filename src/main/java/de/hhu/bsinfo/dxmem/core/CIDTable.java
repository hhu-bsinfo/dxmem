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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxutils.ArrayListLong;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Paging-like Tables for the ChunkID-VA mapping
 *
 * @author Florian Klein, florian.klein@hhu.de, 13.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 * @author Florian Hucke, florian.hucke@hhu.de, 06.02.2018
 */
public final class CIDTable implements Importable, Exportable {
    private static final Logger LOGGER = LogManager.getFormatterLogger(CIDTable.class.getSimpleName());

    static final int TABLE_0_ALIGNMENT_BYTES = 8;
    static final byte ENTRY_SIZE = 8;
    static final byte LID_TABLE_LEVELS = 4;

    private static final byte BITS_FOR_NID_LEVEL = 16;
    static final byte BITS_PER_LID_LEVEL = 48 / LID_TABLE_LEVELS;

    static final int ENTRIES_PER_NID_LEVEL = (int) Math.pow(2.0, BITS_FOR_NID_LEVEL);
    private static final int NID_TABLE_SIZE = ENTRY_SIZE * ENTRIES_PER_NID_LEVEL;
    private static final long NID_LEVEL_BITMASK = (int) Math.pow(2.0, BITS_FOR_NID_LEVEL) - 1;

    static final int ENTRIES_PER_LID_LEVEL = (int) Math.pow(2.0, BITS_PER_LID_LEVEL);
    private static final int LID_TABLE_SIZE = ENTRY_SIZE * ENTRIES_PER_LID_LEVEL;
    private static final long LID_LEVEL_BITMASK = (int) Math.pow(2.0, BITS_PER_LID_LEVEL) - 1;

    private short m_ownNodeId;
    private long m_addressTableDirectory;
    private CIDTableStatus m_status = new CIDTableStatus();

    CIDTranslationCache m_cidTranslationCache;
    Heap m_heap;

    // lock to protect CID table on table creation
    private final ReentrantLock m_tableManagementLock = new ReentrantLock(false);

    /**
     * Constructor
     * Create a new (empty) CIDTable
     *
     * @param p_ownNodeId
     *         Node id of current instance
     * @param p_heap
     *         Heap instance
     * @param p_cidTranslationCache
     *         CIDTranslationCache instance
     */
    CIDTable(final short p_ownNodeId, final Heap p_heap, final CIDTranslationCache p_cidTranslationCache) {
        m_ownNodeId = p_ownNodeId;
        m_heap = p_heap;
        m_cidTranslationCache = p_cidTranslationCache;

        CIDTableTableEntry entry = new CIDTableTableEntry();
        createNIDTable(entry);

        m_addressTableDirectory = entry.getAddress();

        LOGGER.info("CIDTable: init success (page directory at: 0x%X)", m_addressTableDirectory);
    }

    /**
     * Constructor
     * Create CIDTable to load from a memory dump file
     */
    CIDTable() {
        LOGGER.info("Created 'invalid' CIDTable for loading dump from file");
    }

    /**
     * Get the node id of the current instance
     *
     * @return Node id of current instance
     */
    public short getOwnNodeId() {
        return m_ownNodeId;
    }

    /**
     * Get the current status of the CIDTable
     *
     * @return Current status of CIDTable
     */
    public CIDTableStatus getStatus() {
        return m_status;
    }

    /**
     * Translate an existing (or non existing) entry without locking the entry
     *
     * @param p_chunkID
     *         Chunk id to translate
     * @param p_entry
     *         Reference to CIDTableChunkEntry object to write results to
     */
    public void translate(final long p_chunkID, final CIDTableChunkEntry p_entry) {
        long index;
        long entry;

        int level = LID_TABLE_LEVELS;
        long addressTable;
        boolean putCache;

        // try to jump to table level 0 using the cache
        addressTable = m_cidTranslationCache.getTableLevel0(p_chunkID);

        if (addressTable != Address.INVALID) {
            level = 0;
            putCache = false;
        } else {
            // not found in cache, start with directory and add address to cache later
            addressTable = m_addressTableDirectory;
            putCache = true;
        }

        do {
            if (level == LID_TABLE_LEVELS) {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & NID_LEVEL_BITMASK;
            } else {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & LID_LEVEL_BITMASK;
            }

            entry = readTableEntry(addressTable, index);

            if (level > 0) {
                // chunk was either deleted or never existed
                if (entry == CIDTableChunkEntry.RAW_VALUE_FREE || entry == CIDTableZombieEntry.RAW_VALUE) {
                    return;
                }

                // move on to next table
                // don't use a temporary CIDTableChunkEntry object here to avoid overhead
                addressTable = CIDTableTableEntry.getAddressOfRawTableEntry(entry);
            } else {
                // add table 0 address to cache
                if (putCache) {
                    m_cidTranslationCache.putTableLevel0(p_chunkID, addressTable);
                }

                // update entry state
                p_entry.set(addressTable + index * ENTRY_SIZE, entry);
                return;
            }

            level--;
        } while (level >= 0);
    }

    /**
     * Insert a new (or overwrite existing) entry
     *
     * @param p_chunkID
     *         Chunk id to insert at
     * @param p_entry
     *         Entry to insert
     * @return True if insert successful, false on error (out of memory for new LID table malloc)
     */
    public boolean insert(final long p_chunkID, final CIDTableChunkEntry p_entry) {
        long index;
        long entry;

        int level = LID_TABLE_LEVELS;
        long addressTable;
        boolean putCache;

        // try to jump to table level 0 using the cache
        addressTable = m_cidTranslationCache.getTableLevel0(p_chunkID);

        if (addressTable != Address.INVALID) {
            level = 0;
            putCache = false;
        } else {
            // not found in cache, start with directory and add address to cache later
            addressTable = m_addressTableDirectory;
            putCache = true;
        }

        do {
            if (level == LID_TABLE_LEVELS) {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & NID_LEVEL_BITMASK;
            } else {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & LID_LEVEL_BITMASK;
            }

            if (level > 0) {
                entry = readTableEntry(addressTable, index);

                if (entry == CIDTableChunkEntry.RAW_VALUE_FREE) {
                    // protect metadata on table creation and avoid concurrent threads creating
                    // the same table multiple times
                    m_tableManagementLock.lock();

                    // double check locking, re-read entry because another thread might have already created
                    // the table when we were waiting
                    entry = readTableEntry(addressTable, index);

                    if (entry == CIDTableChunkEntry.RAW_VALUE_FREE) {
                        CIDTableTableEntry tmpEntry = new CIDTableTableEntry();

                        if (!createLIDTable(tmpEntry, level - 1)) {
                            // out of memory abort insert
                            return false;
                        }

                        writeTableEntry(addressTable, index, tmpEntry.getValue());

                        m_tableManagementLock.unlock();

                        // move to next (and newly) created table
                        addressTable = tmpEntry.getAddress();
                    } else {
                        m_tableManagementLock.unlock();

                        addressTable = CIDTableTableEntry.getAddressOfRawTableEntry(entry);
                    }
                } else {
                    // move on to next table
                    // don't use a temporary CIDTableChunkEntry object here to avoid overhead
                    addressTable = CIDTableTableEntry.getAddressOfRawTableEntry(entry);
                }
            } else {
                // add table 0 address to cache to profit from consecutive inserts or lookups
                if (putCache) {
                    m_cidTranslationCache.putTableLevel0(p_chunkID, addressTable);
                }

                // if a new chunk is created, the LIDStore ensures that it doesn't hand out the same chunk ID
                // multiple times. on creates, this guarantees that the new cid is owned by a single thread, only.
                // thus, no locking or atomic update is required here
                writeTableEntry(addressTable, index, p_entry.getValue());

                // update entry state
                p_entry.setPointer(addressTable + index * ENTRY_SIZE);
                return true;
            }

            level--;
        } while (level >= 0);

        return true;
    }

    /**
     * Re-read the entry. Necessary if atomic updates failed
     *
     * @param p_entry
     *         Entry to re-read
     */
    public void entryReread(final CIDTableChunkEntry p_entry) {
        p_entry.set(p_entry.getPointer(), m_heap.readLong(p_entry.getPointer(), 0));
    }

    /**
     * Non atomic update to be used if entry is write locked anyway
     *
     * @param p_entry
     *         Entry to update
     */
    public void entryUpdate(final CIDTableChunkEntry p_entry) {
        m_heap.writeLong(p_entry.getPointer(), 0, p_entry.getValue());
    }

    /**
     * Flag an entry as free (non atomic update for a delete operation)
     *
     * @param p_entry
     *         Entry to flag as free
     */
    public void entryFlagFree(final CIDTableChunkEntry p_entry) {
        m_heap.writeLong(p_entry.getPointer(), 0, CIDTableChunkEntry.RAW_VALUE_FREE);
    }

    /**
     * Flag an entry as zombie (non atomic update for a delete operation)
     *
     * @param p_entry
     *         Entry to flag as zombie
     */
    public void entryFlagZombie(final CIDTableChunkEntry p_entry) {
        m_heap.writeLong(p_entry.getPointer(), 0, CIDTableZombieEntry.RAW_VALUE);
    }

    /**
     * Try an atomic update of an altered entry in memory
     *
     * @param p_entry
     *         Entry to update
     * @return True if update successful, false otherwise. Entry is not automatically re-read
     */
    public boolean entryAtomicUpdate(final CIDTableChunkEntry p_entry) {
        return m_heap.casLong(p_entry.getPointer(), 0, p_entry.getInitalValue(), p_entry.getValue());
    }

    /**
     * Returns the ChunkID ranges of all local and migrated chunks
     *
     * @return the ChunkID ranges
     */
    public ChunkIDRanges getCIDRangesOfAllChunks() {
        ArrayListLong ret;
        long entry;

        ret = new ArrayListLong();

        for (int i = 0; i < ENTRIES_PER_NID_LEVEL; i++) {
            entry = readTableEntry(m_addressTableDirectory, i);

            if (entry > 0) {
                getAllRanges(ret, (long) i << 48, entry, LID_TABLE_LEVELS - 1);
            }
        }

        return ChunkIDRanges.wrap(ret);
    }

    /**
     * Returns the ChunkID ranges of all locally stored Chunks
     *
     * @param p_nodeId
     *         Node id of the remote to get the data from
     * @return the ChunkID ranges
     */
    public ChunkIDRanges getCIDRangesOfAllChunks(final short p_nodeId) {
        ArrayListLong ret = new ArrayListLong();
        long entry = readTableEntry(m_addressTableDirectory, p_nodeId & 0xFFFF);

        if (entry > 0) {
            getAllRanges(ret, ChunkID.getChunkID(p_nodeId, 0), entry, LID_TABLE_LEVELS - 1);
        }

        return ChunkIDRanges.wrap(ret);
    }

    /**
     * Get all ranges of all migrated chunks
     *
     * @return Chunk ID ranges of all migrated chunks
     */
    public ChunkIDRanges getCIDRangesOfAllMigratedChunks() {
        ArrayListLong ret;
        long entry;

        ret = new ArrayListLong();

        for (int i = 0; i < ENTRIES_PER_NID_LEVEL; i++) {
            if (i != (m_ownNodeId & 0xFFFF)) {
                entry = readTableEntry(m_addressTableDirectory, i);

                if (entry > 0) {
                    getAllRanges(ret, (long) i << 48, entry, LID_TABLE_LEVELS - 1);
                }
            }
        }

        return ChunkIDRanges.wrap(ret);
    }

    @Override
    public String toString() {
        return "CIDTable: m_ownNodeId " + NodeID.toHexString(m_ownNodeId) + ", m_addressTableDirectory " +
                Address.toHexString(m_addressTableDirectory) + ", " + m_status;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeShort(m_ownNodeId);
        p_exporter.writeLong(m_addressTableDirectory);
        p_exporter.exportObject(m_status);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_ownNodeId = p_importer.readShort(m_ownNodeId);
        m_addressTableDirectory = p_importer.readLong(m_addressTableDirectory);
        p_importer.importObject(m_status);
    }

    @Override
    public int sizeofObject() {
        return Short.BYTES + Long.BYTES + m_status.sizeofObject() + Long.BYTES;
    }

    /**
     * Adds all ChunkID ranges to an ArrayList
     *
     * @param p_unfinishedCID
     *         the unfinished ChunkID
     * @param p_table
     *         the current table
     * @param p_level
     *         the current table level
     */
    private void getAllRanges(final ArrayListLong p_ret, final long p_unfinishedCID, final long p_table,
            final int p_level) {
        long entry;

        for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
            entry = readTableEntry(p_table, i);

            if (entry != CIDTableChunkEntry.RAW_VALUE_FREE && entry != CIDTableZombieEntry.RAW_VALUE) {
                if (p_level > 0) {
                    getAllRanges(p_ret, p_unfinishedCID + (i << BITS_PER_LID_LEVEL * p_level),
                            CIDTableTableEntry.getAddressOfRawTableEntry(entry), p_level - 1);
                } else {
                    long curCID = p_unfinishedCID + i;

                    if (p_ret.getSize() < 2) {
                        p_ret.add(curCID);
                        p_ret.add(curCID);
                    } else {
                        long prev = p_ret.get(p_ret.getSize() - 1);

                        if (prev + 1 == curCID) {
                            p_ret.set(p_ret.getSize() - 1, curCID);
                        } else {
                            p_ret.add(curCID);
                            p_ret.add(curCID);
                        }
                    }
                }
            }
        }
    }

    /**
     * Inverse search for CID to chunk address. Used for unpinning chunks with pinned addresses
     *
     * @param p_entry
     *         Reference to write results to
     * @param p_chunkAddress
     *         Address of pinned chunk
     * @return CID for address, or invalid CID if not found.
     */
    public long getTableEntryWithChunkAddress(final CIDTableChunkEntry p_entry, final long p_chunkAddress) {
        p_entry.clear();
        p_entry.setAddress(p_chunkAddress);

        long cid = getTableEntryWithChunkAddressRecursive(p_entry, 0, m_addressTableDirectory, LID_TABLE_LEVELS);

        if (cid == ChunkID.INVALID_ID) {
            p_entry.clear();
        }

        return cid;
    }

    /**
     * Search, get and remove zombie entries. Used in LIDStore when re-using CIDs
     *
     * @param p_nodeId
     *         Node id of table to start searching in
     * @param p_ringBuffer
     *         Ring buffer to write found zombies to
     * @param p_offset
     *         Offset in ring buffer where to start
     * @param p_maxCount
     *         Max number of elements to search
     * @return Number of zombies found
     */
    int getAndEliminateZombies(final short p_nodeId, final long[] p_ringBuffer, final int p_offset,
            final int p_maxCount) {
        int count = 0;
        long entry = readTableEntry(m_addressTableDirectory, p_nodeId & 0xFFFF);

        if (entry != CIDTableTableEntry.RAW_VALUE_FREE) {
            count += getAndEliminateZombiesRecursiveLIDTable(CIDTableTableEntry.getAddressOfRawTableEntry(entry),
                    LID_TABLE_LEVELS - 1, 0, 0, p_ringBuffer, p_offset, p_maxCount);
        }

        return count;
    }

    // for debugging (analyzer)
    // pass null for list references to not harvest entries for that category

    /**
     * For debugging (analyzer): deep scan all tables. Pass null for list references to omit harvesting that category
     *
     * @param p_tableEntries
     *         Reference to ArrayList to return found table entries to
     * @param p_chunkEntries
     *         Reference to ArrayList to return found chunk entries to
     * @param p_zombieEntries
     *         Reference to ArrayList to return found zombie entries to
     */
    void scanAllTables(final ArrayList<CIDTableTableEntry> p_tableEntries,
            final ArrayList<CIDTableChunkEntry> p_chunkEntries, final ArrayList<CIDTableZombieEntry> p_zombieEntries) {
        // root table
        if (p_tableEntries != null) {
            p_tableEntries.add(new CIDTableTableEntry(0, m_addressTableDirectory));
        }

        for (int nid = 0; nid < ENTRIES_PER_NID_LEVEL; nid++) {
            long entry = readTableEntry(m_addressTableDirectory, nid);

            if (entry != CIDTableTableEntry.RAW_VALUE_FREE) {
                if (p_tableEntries != null) {
                    p_tableEntries.add(new CIDTableTableEntry(m_addressTableDirectory + nid * ENTRY_SIZE, entry));
                }

                scanRecursiveLIDTable(CIDTableTableEntry.getAddressOfRawTableEntry(entry), LID_TABLE_LEVELS - 1,
                        ChunkID.getChunkID((short) nid, 0), p_tableEntries, p_chunkEntries, p_zombieEntries);
            }
        }
    }

    /**
     * For debugging and heap analysis. Find heap area where table entry is stored
     *
     * @param p_entry
     *         Table entry to search
     * @return HeapArea which points to memory where table is stored in memory
     */
    HeapArea scanCIDTableEntry(final CIDTableTableEntry p_entry) {
        int tableSize;

        if (p_entry.getPointer() == Address.INVALID) {
            // NID (root) table
            tableSize = CIDTable.NID_TABLE_SIZE;
        } else {
            // LID table
            tableSize = CIDTable.LID_TABLE_SIZE;
        }

        // heap areas start with front marker (including) and end with end marker (excluding)
        return new HeapArea(p_entry.getAddress() - 1, p_entry.getAddress() + tableSize);
    }

    /**
     * Get and remove zombie entries from lid table (recursive call)
     *
     * @param p_addressTable
     *         Address of table to iterate
     * @param p_level
     *         Current table level
     * @param p_cid
     *         Current cid
     * @param p_currentCount
     *         Zombies found so far by child calls
     * @param p_ringBuffer
     *         Reference to ring buffer to write zombies to
     * @param p_offset
     *         Offset in ring buffer where to start writing to
     * @param p_maxCount
     *         Max number of zombies to get
     * @return Number of zombies found and removed
     */
    private int getAndEliminateZombiesRecursiveLIDTable(final long p_addressTable, final int p_level, final long p_cid,
            final int p_currentCount, final long[] p_ringBuffer, final int p_offset, final int p_maxCount) {
        int count = 0;

        for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
            if (p_currentCount + count == p_maxCount) {
                return count;
            }

            long entry = readTableEntry(p_addressTable, i);

            if (p_level != 0) {
                if (entry != CIDTableTableEntry.RAW_VALUE_FREE) {
                    count += getAndEliminateZombiesRecursiveLIDTable(
                            CIDTableTableEntry.getAddressOfRawTableEntry(entry), p_level - 1,
                            p_cid | i << p_level * BITS_PER_LID_LEVEL, p_currentCount + count, p_ringBuffer, p_offset,
                            p_maxCount);
                }
            } else {
                long cid = p_cid | i << p_level * BITS_PER_LID_LEVEL;

                if (entry == CIDTableZombieEntry.RAW_VALUE) {
                    p_ringBuffer[(p_offset + p_currentCount + count) % p_maxCount] = cid;

                    // delete zombie entry
                    writeTableEntry(p_addressTable, i, CIDTableTableEntry.RAW_VALUE_FREE);
                    count++;
                }
            }
        }

        return count;
    }

    /**
     * Recursive call for deep scan of cid table (debugging/analysis)
     *
     * @param p_addressTable
     *         Address of table to iterate
     * @param p_level
     *         Current table level
     * @param p_cid
     *         Current cid
     * @param p_tableEntries
     *         Reference to ArrayList to return found table entries to
     * @param p_chunkEntries
     *         Reference to ArrayList to return found chunk entries to
     * @param p_zombieEntries
     *         Reference to ArrayList to return found zombie entries to
     */
    private void scanRecursiveLIDTable(final long p_addressTable, final int p_level, final long p_cid,
            final ArrayList<CIDTableTableEntry> p_tableEntries, final ArrayList<CIDTableChunkEntry> p_chunkEntries,
            final ArrayList<CIDTableZombieEntry> p_zombieEntries) {
        for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
            long entry = readTableEntry(p_addressTable, i);

            if (p_level != 0) {
                if (entry != CIDTableTableEntry.RAW_VALUE_FREE) {
                    if (p_tableEntries != null) {
                        p_tableEntries.add(new CIDTableTableEntry(p_addressTable + i * ENTRY_SIZE, entry));
                    }

                    scanRecursiveLIDTable(CIDTableTableEntry.getAddressOfRawTableEntry(entry), p_level - 1,
                            p_cid | i << p_level * BITS_PER_LID_LEVEL, p_tableEntries, p_chunkEntries, p_zombieEntries);
                }
            } else {
                long cid = p_cid | i << p_level * BITS_PER_LID_LEVEL;

                if (entry != CIDTableChunkEntry.RAW_VALUE_FREE) {
                    if (entry != CIDTableZombieEntry.RAW_VALUE) {
                        if (p_chunkEntries != null) {
                            p_chunkEntries.add(new CIDTableChunkEntry(p_addressTable + i * ENTRY_SIZE, entry));
                        }
                    } else {
                        if (p_zombieEntries != null) {
                            p_zombieEntries.add(new CIDTableZombieEntry(p_addressTable + i * ENTRY_SIZE, cid));
                        }
                    }
                }
            }
        }
    }

    /**
     * Creates the NodeID table
     *
     * @param p_entry
     *         Reference to table entry to write results to
     */
    private void createNIDTable(final CIDTableTableEntry p_entry) {
        CIDTableChunkEntry tmp = new CIDTableChunkEntry();

        // no need to store any length information
        tmp.setLengthField(0);

        // TODO reduce pointer length to 6 byte because TableEntry is address only
        if (!m_heap.malloc(NID_TABLE_SIZE, tmp, true)) {
            throw new MemoryRuntimeException("Creating the NID table should not fail");
        }

        p_entry.clear();
        p_entry.setAddress(tmp.getAddress());

        // null table
        m_heap.set(p_entry.getAddress(), 0, NID_TABLE_SIZE, (byte) 0);
        m_status.m_totalPayloadMemoryTables += NID_TABLE_SIZE;
        m_status.m_totalTableCount++;

        LOGGER.trace("Created NID table size %d at %X", NID_TABLE_SIZE, p_entry.getAddress());
    }

    /**
     * Creates a LID table
     *
     * @param p_entry
     *         Reference to table entry to write results to
     * @param p_level
     *         Table level to create
     * @return True on success, false on out of memory
     */
    private boolean createLIDTable(final CIDTableTableEntry p_entry, final int p_level) {
        CIDTableChunkEntry tmp = new CIDTableChunkEntry();

        // no need to store any length information
        tmp.setLengthField(0);

        // TODO reduce pointer length for all levels except 0 to 6 byte because TableEntry is address only

        int alignment = 0;

        // align all level 0 tables containing locks to 8 byte borders
        // otherwise, the latency for CAS operations sky rockets
        if (p_level == 0) {
            alignment = TABLE_0_ALIGNMENT_BYTES;
        }

        if (!m_heap.malloc(LID_TABLE_SIZE + alignment, tmp, true)) {
            return false;
        }

        if (alignment > 0) {
            alignment = -1;

            for (int i = 0; i < TABLE_0_ALIGNMENT_BYTES; i++) {
                if ((tmp.getAddress() + i) % TABLE_0_ALIGNMENT_BYTES == 0) {
                    alignment = i;
                    break;
                }
            }

            if (alignment == -1) {
                throw new IllegalStateException("Alignment failed for address: " + tmp.getAddress());
            }
        }

        // null table
        m_heap.set(tmp.getAddress(), 0, LID_TABLE_SIZE + alignment, (byte) 0);

        // expose aligned origin only
        tmp.setAddress(tmp.getAddress() + alignment);

        p_entry.clear();
        p_entry.setAddress(tmp.getAddress());

        m_status.m_totalPayloadMemoryTables += LID_TABLE_SIZE + alignment;
        m_status.m_tableCountLevel[p_level]++;
        m_status.m_totalTableCount++;

        LOGGER.trace("Created LID table size %d at %X (alignment offset +" + alignment, LID_TABLE_SIZE + alignment,
                p_entry.getAddress());

        return true;
    }

    /**
     * Reads a table entry
     *
     * @param p_addressTable
     *         the table
     * @param p_index
     *         the index of the entry
     * @return the entry
     */
    private long readTableEntry(final long p_addressTable, final long p_index) {
        return m_heap.readLong(p_addressTable, p_index * ENTRY_SIZE);
    }

    /**
     * Writes a table entry
     *
     * @param p_addressTable
     *         the table
     * @param p_index
     *         the index of the entry
     * @param p_entry
     *         the entry
     */
    private void writeTableEntry(final long p_addressTable, final long p_index, final long p_entry) {
        m_heap.writeLong(p_addressTable, p_index * ENTRY_SIZE, p_entry);
    }

    /**
     * Calculate the absolute memory address of a table entry
     *
     * @param p_addressTable
     *         Base address of table
     * @param p_index
     *         Index into table
     * @return Absolute memory address of entry in table
     */
    private long calcAddressTableEntry(final long p_addressTable, final long p_index) {
        return p_addressTable + p_index * ENTRY_SIZE;
    }

    /**
     * Find a table entry using the address (recursive call)
     *
     * @param p_entry
     *         Reference to entry to write results to
     * @param p_cid
     *         Current cid
     * @param p_addressTable
     *         Address of current table
     * @param p_level
     *         Current level of table
     * @return CID of address if found, invalid CID if not found
     */
    private long getTableEntryWithChunkAddressRecursive(final CIDTableChunkEntry p_entry, final long p_cid,
            final long p_addressTable, final int p_level) {
        if (p_addressTable == Address.INVALID) {
            return ChunkID.INVALID_ID;
        }

        if (p_level == LID_TABLE_LEVELS) {
            // nid tables
            for (int i = 0; i < ENTRIES_PER_NID_LEVEL; i++) {
                long entry = readTableEntry(p_addressTable, i);

                long cid = getTableEntryWithChunkAddressRecursive(p_entry, p_cid | i << p_level * BITS_PER_LID_LEVEL,
                        entry, LID_TABLE_LEVELS - 1);

                if (cid != ChunkID.INVALID_ID) {
                    return cid;
                }
            }
        } else {
            for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
                long entry = readTableEntry(p_addressTable, i);

                // lid tables
                if (p_level > 0) {
                    long cid = getTableEntryWithChunkAddressRecursive(p_entry,
                            p_cid | i << p_level * BITS_PER_LID_LEVEL, entry, p_level - 1);

                    if (cid != ChunkID.INVALID_ID) {
                        return cid;
                    }
                } else {
                    // level 0 table with chunk entries
                    if (CIDTableChunkEntry.getAddressOfRawEntry(entry) == p_entry.getAddress()) {
                        long cid = p_cid | i << p_level * BITS_PER_LID_LEVEL;

                        // found, abort search
                        p_entry.set(calcAddressTableEntry(p_addressTable, i), entry);
                        return cid;
                    }
                }
            }
        }

        return ChunkID.INVALID_ID;
    }
}
