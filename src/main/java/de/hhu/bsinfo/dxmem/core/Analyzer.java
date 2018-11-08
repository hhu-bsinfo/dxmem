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
import java.util.Comparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;

/**
 * ToolAnalyzer which scans and verifies the heap and CID table structures to detect errors
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public class Analyzer {
    private static final Logger LOGGER = LogManager.getFormatterLogger(Analyzer.class.getSimpleName());

    private Heap m_heap;
    private CIDTable m_cidTable;

    private ArrayList<CIDTableTableEntry> m_cidTableTableEntries;
    private ArrayList<CIDTableChunkEntry> m_cidTableChunkEntries;
    private ArrayList<CIDTableZombieEntry> m_cidTableZombieEntries;

    private ArrayList<HeapArea> m_heapTables;
    private ArrayList<HeapArea> m_heapChunks;
    private ArrayList<HeapArea> m_heapFree;
    private ArrayList<HeapArea> m_heapFragmented;

    /**
     * Constructor
     *
     * @param p_heap
     *         Heap instance to analyze
     * @param p_cidTable
     *         CIDTable instance to analyze
     */
    public Analyzer(final Heap p_heap, final CIDTable p_cidTable) {
        m_heap = p_heap;
        m_cidTable = p_cidTable;

        m_cidTableTableEntries = new ArrayList<>();
        m_cidTableChunkEntries = new ArrayList<>();
        m_cidTableZombieEntries = new ArrayList<>();

        m_heapTables = new ArrayList<>();
        m_heapChunks = new ArrayList<>();
        m_heapFree = new ArrayList<>();
        m_heapFragmented = new ArrayList<>();
    }

    /**
     * Analyze the heap and cid table
     *
     * @return True if successful, false on errors
     */
    public boolean analyze() {
        // TODO analyze heap with cid table in multiple steps
        // 1. cid table scan: gather all entries from the cid table using a scan
        // 2. cid table verify: check all entries of the table if they are valid entries and match end results of
        // counters against status (total entries used etc)
        // 3. heap scan (1): gather used blocks from heap using the cid entries gather before
        // 4. heap scan (2): gather free blocks from heap
        // 5. heap verify: check if the heap does not contain any gaps by walking the gathered entries. also keep track
        // of allocated blocks/size etc and match against the current status of the heap
        m_cidTableTableEntries.clear();
        m_cidTableChunkEntries.clear();
        m_cidTableZombieEntries.clear();

        m_heapTables.clear();
        m_heapChunks.clear();
        m_heapFree.clear();
        m_heapFragmented.clear();

        return cidTableScan() && cidTableVerify() && heapScan1() && heapScan2() && heapVerify();
    }

    /**
     * Get the collected CIDTable table entries of the analysis
     *
     * @return Collected CIDTable table entries
     */
    public ArrayList<CIDTableTableEntry> getCIDTableTableEntries() {
        return m_cidTableTableEntries;
    }

    /**
     * Get the collected CIDTable chunk entries of the analysis
     *
     * @return Collected CIDTable chunk entries
     */
    public ArrayList<CIDTableChunkEntry> getCIDTableChunkEntries() {
        return m_cidTableChunkEntries;
    }

    /**
     * Get the collected CIDTable zombie entries of the analysis
     *
     * @return Collected CIDTable zombie entries
     */
    public ArrayList<CIDTableZombieEntry> getCIDTableZombieEntries() {
        return m_cidTableZombieEntries;
    }

    /**
     * Scan the CIDTable
     *
     * @return True on success, false on error
     */
    private boolean cidTableScan() {
        LOGGER.debug("Scanning CID table...");

        m_cidTable.scanAllTables(m_cidTableTableEntries, m_cidTableChunkEntries, m_cidTableZombieEntries);

        LOGGER.debug("Done");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("CID table contents:");
            LOGGER.trace("Table entries (%d):", m_cidTableTableEntries.size());

            for (CIDTableTableEntry entry : m_cidTableTableEntries) {
                LOGGER.trace(entry);
            }

            LOGGER.trace("Chunk entries (%d):", m_cidTableChunkEntries.size());

            for (CIDTableChunkEntry entry : m_cidTableChunkEntries) {
                LOGGER.trace(entry);
            }

            LOGGER.trace("Zombie entries (%d):", m_cidTableZombieEntries.size());

            for (CIDTableZombieEntry entry : m_cidTableZombieEntries) {
                LOGGER.trace(entry);
            }
        }

        return true;
    }

    /**
     * Verify the CIDTable
     *
     * @return True on success, false on error
     */
    private boolean cidTableVerify() {
        LOGGER.debug("Verifying table entries (%d)...", m_cidTableTableEntries.size());

        for (CIDTableTableEntry entry : m_cidTableTableEntries) {
            // only the root table does not have a valid pointer
            // consider that root table is aligned to 64-bit bounds
            if (entry.getPointer() == Address.INVALID && (entry.getAddress() == 0x00 || entry.getAddress() > 0x09)) {
                LOGGER.error("Invalid pointer value for table entry: %s", entry);
                return false;
            }

            if (!entry.isAddressValid()) {
                LOGGER.error("Invalid address for table entry: %s", entry);
            }
        }

        LOGGER.debug("Done");

        LOGGER.debug("Verifying chunk entries (%d)...", m_cidTableChunkEntries.size());

        for (CIDTableChunkEntry entry : m_cidTableChunkEntries) {
            if (!entry.isValid()) {
                LOGGER.error("Invalid chunk entry: %s", entry);
                return false;
            }

            if (!entry.isAddressValid()) {
                LOGGER.error("Invalid address for chunk entry: %s", entry);
                return false;
            }

            if (entry.getInitalValue() == 0) {
                LOGGER.error("Invalid initial value for chunk entry: %s", entry);
                return false;
            }
        }

        LOGGER.debug("Done");

        LOGGER.debug("Verifying zombie entries (%d)...", m_cidTableZombieEntries.size());

        for (CIDTableZombieEntry entry : m_cidTableZombieEntries) {
            if (entry.getPointer() == Address.INVALID) {
                LOGGER.error("Invalid pointer for zombie entry: %s", entry);
                return false;
            }

            if (entry.getCID() == ChunkID.INVALID_ID) {
                LOGGER.error("Invalid chunk id for zombie entry: %s", entry);
                return false;
            }
        }

        LOGGER.debug("Done");

        // TODO verify against status counters

        return true;
    }

    /**
     * Execute first phase of heap scan
     *
     * @return True on success, false on error
     */
    private boolean heapScan1() {
        LOGGER.debug("Heap scan tables...");

        for (CIDTableTableEntry entry : m_cidTableTableEntries) {
            m_heapTables.add(m_cidTable.scanCIDTableEntry(entry));
        }

        LOGGER.debug("Done");

        LOGGER.debug("Heap scan used chunks...");

        for (CIDTableChunkEntry entry : m_cidTableChunkEntries) {
            m_heapChunks.add(m_heap.scanChunkEntry(entry));
        }

        LOGGER.debug("Done");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Heap areas tables (%d):", m_heapTables.size());

            for (HeapArea area : m_heapTables) {
                LOGGER.trace(area);
            }

            LOGGER.trace("Heap areas chunks (%d):", m_heapChunks.size());

            for (HeapArea area : m_heapChunks) {
                LOGGER.trace(area);
            }
        }

        return true;
    }

    /**
     * Execute second phase of heap scan
     *
     * @return True on success, false on error
     */
    private boolean heapScan2() {
        LOGGER.debug("Heap scan free block lists...");

        m_heapFree = m_heap.scanFreeBlockLists();

        LOGGER.debug("Done");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Heap free (tracked) blocks (%d):", m_heapFree.size());

            for (HeapArea area : m_heapFree) {
                LOGGER.trace(area);
            }
        }

        return true;
    }

    /**
     * Verify the heap
     *
     * @return True on success, false on error
     */
    private boolean heapVerify() {
        LOGGER.debug("Heap verify, sorting addresses...");

        // sort all lists ascending by start address
        m_heapTables.sort(Comparator.comparing(HeapArea::getStartAddress));
        m_heapChunks.sort(Comparator.comparing(HeapArea::getStartAddress));
        m_heapFree.sort(Comparator.comparing(HeapArea::getStartAddress));

        LOGGER.debug("Done");

        // start with root nid table
        if (m_heapTables.isEmpty()) {
            LOGGER.error("Missing root CID table");
            return false;
        }

        HeapArea curArea = m_heapTables.get(0);

        // consider 64-bit alignment for root table
        if (curArea.getStartAddress() >= 8) {
            LOGGER.error("First table is not root table: %s", curArea);
            return false;
        }

        LOGGER.debug("Iterating and verifying heap...");

        LOGGER.trace("%s root NID table", curArea);

        HeapArea nextArea = null;
        // = 1: skip root table
        int posHeapTables = 1;
        int posHeapChunks = 0;
        int posHeapFree = 0;

        while (true) {
            // try to find next area by checking all lists
            if (posHeapTables < m_heapTables.size()) {
                nextArea = m_heapTables.get(posHeapTables);
            }

            if (nextArea != null && curArea.getEndAddress() == nextArea.getStartAddress()) {
                posHeapTables++;
                LOGGER.trace("%s LID table", nextArea);
            } else {
                if (posHeapChunks < m_heapChunks.size()) {
                    nextArea = m_heapChunks.get(posHeapChunks);
                }

                if (nextArea != null && curArea.getEndAddress() == nextArea.getStartAddress()) {
                    posHeapChunks++;
                    LOGGER.trace("%s allocated block (full blocksize %d)", nextArea,
                            nextArea.getEndAddress() - nextArea.getStartAddress() - 1);
                } else {
                    if (posHeapFree < m_heapFree.size()) {
                        nextArea = m_heapFree.get(posHeapFree);
                    }

                    if (nextArea != null && curArea.getEndAddress() == nextArea.getStartAddress()) {
                        posHeapFree++;
                        LOGGER.trace("%s free tracked block (full blocksize: %d)", nextArea,
                                nextArea.getEndAddress() - nextArea.getStartAddress() - 1);
                    } else {
                        // next block is no table, chunk or (tracked) free block
                        // check if it's a fragmented free block, otherwise we got an invalid gap
                        byte rightMarker = (byte) m_heap.readRightPartOfMarker(curArea.getEndAddress());

                        if (rightMarker == Heap.SINGLE_BYTE_MARKER) {
                            nextArea = new HeapArea(curArea.getEndAddress() + 1, curArea.getEndAddress() + 1);
                            LOGGER.trace("%s single byte marker", nextArea);
                            m_heapFragmented.add(nextArea);
                        } else if (rightMarker == Heap.UNTRACKED_FREE_BLOCK_MARKER) {
                            int size = m_heap.readByte(curArea.getEndAddress(), 1);

                            if (size == 0) {
                                LOGGER.error("Found untracked block with invalid size 0 at %X",
                                        curArea.getEndAddress() + 1);
                                return false;
                            }

                            nextArea = new HeapArea(curArea.getEndAddress() + 1, curArea.getEndAddress() + 1 + size);
                            LOGGER.trace("%s untracked free block, size %d", nextArea, size);
                            m_heapFragmented.add(nextArea);
                        } else if (rightMarker == Heap.HEAP_BORDER_MARKER) {
                            LOGGER.trace("Found end of heap area at %X", curArea.getEndAddress());
                            break;
                        } else {
                            LOGGER.error("Invalid or non gathered next block detected at address %X, marker %d",
                                    curArea.getEndAddress(), rightMarker);
                            return false;
                        }
                    }
                }
            }

            curArea = nextArea;
            nextArea = null;
        }

        LOGGER.trace("Done");

        return true;
    }
}
