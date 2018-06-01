package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.mem.exceptions.MemoryRuntimeException;
import de.hhu.bsinfo.dxram.mem.storage.StorageUnsafeMemory;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.ADDRESS;

/**
 * Managing memory accesses
 *
 * All operations are Thread-Safe
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 28.02.18
 * @projectname dxram-memory
 */
@SuppressWarnings("unused")
public class MemoryManager {
    private SmallObjectHeapDataStructureImExporter[] m_imexporter = new SmallObjectHeapDataStructureImExporter[65536];

    final SmallObjectHeap smallObjectHeap;
    final CIDTable cidTable;
    final MemoryAccess access;
    final MemoryDirectAccess directAccess;
    final MemoryInformation info;
    final MemoryManagement management;
    final MemoryPinning pinning;
    final MemoryAnalyzer analyzer;

    private boolean m_readLock = true;
    private boolean m_writeLock = true;
    private boolean m_doReadLock = true;
    private boolean m_doWriteLock = true;

    public MemoryManager(final short p_nodeID, final long p_heapSize, final int p_maxBlockSize) {
        smallObjectHeap = new SmallObjectHeap(new StorageUnsafeMemory(), p_heapSize, p_maxBlockSize);
        cidTable = new CIDTable(p_nodeID);
        cidTable.initialize(smallObjectHeap);
        access = new MemoryAccess(this);
        directAccess = new MemoryDirectAccess(this);
        info = new MemoryInformation(this);
        management = new MemoryManagement(this);
        pinning = new MemoryPinning(this);
        analyzer = new MemoryAnalyzer(this);
        info.numActiveChunks = 0;
        info.totalActiveChunkMemory = 0;
    }

    //Manage------------------------------------------------------------------------------------------------------------

    /**
     * Check the heap for errors
     *
     * @param quiet Print only errors
     * @param dumpOnError Dump the heap on a error
     * @return True if no error was found, else false
     */
    public boolean checkForErrors(final boolean quiet, final boolean dumpOnError){
        return new MemoryAnalyzer(this).analyze(quiet, dumpOnError);
    }

    /**
     * Shut down the memory manager
     */
    public void shutdownMemory() {
        cidTable.disengage();
        smallObjectHeap.destroy();
    }


    /**
     * Pooling the im/exporters to lower memory footprint.
     *
     * @param p_address
     *         Start address of the chunk
     * @return Im/Exporter for the chunk
     */
    SmallObjectHeapDataStructureImExporter getImExporter(final long p_address) {
        long tid = Thread.currentThread().getId();
        if (tid > 65536) {
            throw new RuntimeException("Exceeded max. thread id");
        }

        // pool the im/exporters
        SmallObjectHeapDataStructureImExporter importer = m_imexporter[(int) tid];
        if (importer == null) {
            m_imexporter[(int) tid] = new SmallObjectHeapDataStructureImExporter(smallObjectHeap, p_address, 0);
            importer = m_imexporter[(int) tid];
        } else {
            importer.setAllocatedMemoryStartAddress(p_address);
            importer.setOffset(0);
        }

        return importer;
    }

    //Locks-------------------------------------------------------------------------------------------------------------

    /**
     * Read lock, this lock is switchable, with the method
     * setLocks.
     *
     *
     * @param p_directEntryAddress
     *          Direct address of the table entry
     * @return
     *          True if a lock is received or on weak consistency else false
     *
     */
    boolean switchableReadLock(final long p_directEntryAddress){
        if (m_doReadLock) {
            if (m_readLock)
                return cidTable.directReadLock(p_directEntryAddress);
            else
                return cidTable.directWriteLock(p_directEntryAddress);
        }

        return true;
    }

    /**
     * Read unlock, this unlock is switchable, with the method
     * setLocks.
     *
     *
     * @param p_directEntryAddress
     *          Direct address of the table entry
     * @return
     *          True if a unlock was successful or on weak consistency, else false
     *
     */
    boolean switchableReadUnlock(final long p_directEntryAddress){
        if (m_doReadLock) {
            if (m_readLock)
                return cidTable.directReadUnlock(p_directEntryAddress);
            else
                return cidTable.directWriteUnlock(p_directEntryAddress);
        }

        return true;
    }

    /**
     * Write lock, this lock is switchable, with the method
     * setLocks.
     *
     *
     * @param p_directEntryAddress
     *          Direct address of the table entry
     * @return
     *          True if a lock is received, else false
     *
     */
    boolean switchableWriteLock(final long p_directEntryAddress){
        if(m_writeLock)
            return cidTable.directWriteLock(p_directEntryAddress);
        else
            return cidTable.directReadLock(p_directEntryAddress);
    }

    /**
     * Write unlock, this unlock is switchable, with the method
     * setLocks.
     *
     *
     * @param p_directEntryAddress
     *          Direct address of the table entry
     * @return
     *          True if a unlock was successful, else false
     *
     */
    boolean switchableWriteUnlock(final long p_directEntryAddress){
        if(m_writeLock)
            return cidTable.directWriteUnlock(p_directEntryAddress);
        else
            return cidTable.directReadUnlock(p_directEntryAddress);
    }


    /**
     * Determine the type of access lock.
     *
     * @param p_readLock
     *          If true, use a read lock, otherwise use a write lock for read operations
     * @param p_writeLock
     *          If true, use a write lock, otherwise use a read lock  for write operations
     */
    final void setLocks(final boolean p_readLock, final boolean p_writeLock) {
        m_readLock = p_readLock;
        m_writeLock = p_writeLock;
    }

    final void disableReadLock(final boolean disableReadLock) {
        m_doReadLock = !disableReadLock;
    }

    final boolean readLockDisabled() {
        return !m_doReadLock;
    }

    final void disableWriteLock(final boolean disableWriteLock) { m_doWriteLock = !disableWriteLock;}

    /**
     * @author Florian Hucke (florian.hucke@hhu.de) on 28.02.18
     * @projectname dxram-memory
     */
    public static class MemoryInformation {
        long numActiveChunks;
        long totalActiveChunkMemory;

        private final SmallObjectHeap m_rawMemory;
        private final CIDTable m_cidTable;


        /**
         * Constructor
         *
         * @param memoryManager
         *          The central unit which manages all memory accesses
         *
         */
        MemoryInformation(MemoryManager memoryManager) {
            this.m_rawMemory = memoryManager.smallObjectHeap;
            m_cidTable = memoryManager.cidTable;
        }

        /**
         * Returns the highest LocalID currently in use
         *
         * @return the LocalID
         */
        public long getHighestUsedLocalID() {
            return m_cidTable.getNextLocalIDCounter() - 1;
        }

        /**
         * Returns the ChunkID ranges of all migrated Chunks
         *
         * @return the ChunkID ranges of all migrated Chunks
         */
        public ChunkIDRanges getCIDRangesOfAllMigratedChunks() {
            return m_cidTable.getCIDRangesOfAllMigratedChunks();
        }

        /**
         * Returns the ChunkID ranges of all locally stored Chunks
         *
         * @return the ChunkID ranges
         */
        public ChunkIDRanges getCIDRangesOfAllLocalChunks() {
            return m_cidTable.getCIDRangesOfAllLocalChunks();
        }

        /**
         * Returns whether this Chunk is stored locally or not.
         * This is an access call and has to be locked using lockAccess().
         *
         * @param p_chunkID
         *         the ChunkID
         * @return whether this Chunk is stored locally or not
         */
        public boolean exists(final long p_chunkID) {
            long address;

            try {
                // Get the address from the CIDTable
                address = ADDRESS.get(m_cidTable.get(p_chunkID));
            } catch (final MemoryRuntimeException e) {
                //handleMemDumpOnError(e, true);
                throw e;
            }

            // If address <= 0, the Chunk does not exists in memory
            return address > SmallObjectHeap.INVALID_ADDRESS &&
                    address < m_rawMemory.m_baseFreeBlockList-SmallObjectHeap.SIZE_MARKER_BYTE;
        }

        /**
         * Returns whether this Chunk was migrated here or not
         *
         * @param p_chunkID
         *         the ChunkID
         * @return whether this Chunk was migrated here or not
         */
        public boolean dataWasMigrated(final long p_chunkID) {
            //->return ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeID();
            return ChunkID.getCreatorID(p_chunkID) != m_cidTable.m_ownNodeID;//<<
        }

        /**
         * Removes the ChunkID of a deleted Chunk that was migrated
         * This is a management call and has to be locked using lockManage().
         *
         * @param p_chunkID
         *         the ChunkID
         */
        public void prepareChunkIDForReuse(final long p_chunkID) {
            // more space for another zombie for reuse in LID store?
            if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID))) {
                // kill zombie entry
                m_cidTable.delete(p_chunkID, false);
            } else {
                // no space for zombie in LID store, keep him "alive" in table
                m_cidTable.delete(p_chunkID, true);
            }
        }


        /**
         * Get some status information about the memory manager (free, total amount of memory).
         *
         * @return Status information.
         */
        public Status getStatus() {
            Status status = new Status();

            status.m_freeMemory = new StorageUnit(m_rawMemory.getStatus().getFree(), StorageUnit.BYTE);
            status.m_maxChunkSize = new StorageUnit(m_rawMemory.getStatus().getMaxBlockSize(), StorageUnit.BYTE);
            status.m_totalMemory = new StorageUnit(m_rawMemory.getStatus().getSize(), StorageUnit.BYTE);
            status.m_totalPayloadMemory = new StorageUnit(m_rawMemory.getStatus().getAllocatedPayload(), StorageUnit.BYTE);
            status.m_numberOfActiveMemoryBlocks = m_rawMemory.getStatus().getAllocatedBlocks();
            status.m_totalChunkPayloadMemory = new StorageUnit(totalActiveChunkMemory, StorageUnit.BYTE);
            status.m_numberOfActiveChunks = numActiveChunks;
            status.m_cidTableCount = m_cidTable.getTableCount();
            status.m_totalMemoryCIDTables = new StorageUnit(m_cidTable.getTotalMemoryTables(), StorageUnit.BYTE);
            status.m_cachedFreeLIDs = m_cidTable.getNumCachedFreeLIDs();
            status.m_availableFreeLIDs = m_cidTable.getNumAvailableFreeLIDs();
            status.m_newLIDCounter = m_cidTable.getNextLocalIDCounter();

            return status;
        }


        /**
         * Status object for the memory component containing various information
         * about it.
         *
         * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.03.2016
         */
        public static class Status implements Importable, Exportable {
            private StorageUnit m_freeMemory;
            private StorageUnit m_maxChunkSize;
            private StorageUnit m_totalMemory;
            private StorageUnit m_totalPayloadMemory;
            private long m_numberOfActiveMemoryBlocks = -1;
            private long m_numberOfActiveChunks = -1;
            private StorageUnit m_totalChunkPayloadMemory;
            private long m_cidTableCount = -1;
            private StorageUnit m_totalMemoryCIDTables;
            private int m_cachedFreeLIDs = -1;
            private long m_availableFreeLIDs = -1;
            private long m_newLIDCounter = -1;

            /**
             * Default constructor
             */
            public Status() {

            }

            /**
             * Get the amount of free memory
             *
             * @return Free memory
             */
            public StorageUnit getFreeMemory() {
                return m_freeMemory;
            }

            /**
             * Get the max allowed chunk size
             *
             * @return Max chunk size
             */
            public StorageUnit getMaxChunkSize() {
                return m_maxChunkSize;
            }

            /**
             * Get the total amount of memory available
             *
             * @return Total amount of memory
             */
            public StorageUnit getTotalMemory() {
                return m_totalMemory;
            }

            /**
             * Get the total number of active/allocated memory blocks
             *
             * @return Number of allocated memory blocks
             */
            public long getNumberOfActiveMemoryBlocks() {
                return m_numberOfActiveMemoryBlocks;
            }

            /**
             * Get the total number of currently active chunks
             *
             * @return Number of active/allocated chunks
             */
            public long getNumberOfActiveChunks() {
                return m_numberOfActiveChunks;
            }

            /**
             * Get the amount of memory used by chunk payload/data
             *
             * @return Amount of memory used by chunk payload
             */
            public StorageUnit getTotalChunkPayloadMemory() {
                return m_totalChunkPayloadMemory;
            }

            /**
             * Get the number of currently allocated CID tables
             *
             * @return Number of CID tables
             */
            public long getCIDTableCount() {
                return m_cidTableCount;
            }

            /**
             * Get the total memory used by CID tables (payload only)
             *
             * @return Total memory used by CID tables
             */
            public StorageUnit getTotalMemoryCIDTables() {
                return m_totalMemoryCIDTables;
            }

            /**
             * Get the total amount of memory allocated and usable for actual payload/data
             *
             * @return Total amount of memory usable for payload
             */
            public StorageUnit getTotalPayloadMemory() {
                return m_totalPayloadMemory;
            }

            /**
             * Get the number of cached LIDs in the LID store
             *
             * @return Number of cached LIDs
             */
            public int getCachedFreeLIDs() {
                return m_cachedFreeLIDs;
            }

            /**
             * Get the number of total available free LIDs of the LIDStore
             *
             * @return Total number of available free LIDs
             */
            public long getAvailableFreeLIDs() {
                return m_availableFreeLIDs;
            }

            /**
             * Get the current state of the counter generating new LIDs
             *
             * @return LID counter state
             */
            public long getNewLIDCounter() {
                return m_newLIDCounter;
            }

            @Override
            public int sizeofObject() {
                return Long.BYTES * 3 + m_freeMemory.sizeofObject() + m_totalMemory.sizeofObject() + m_totalPayloadMemory.sizeofObject() +
                        m_totalChunkPayloadMemory.sizeofObject() + m_totalMemoryCIDTables.sizeofObject() + Integer.BYTES + Long.BYTES * 2;
            }

            @Override
            public void exportObject(final Exporter p_exporter) {
                p_exporter.exportObject(m_freeMemory);
                p_exporter.exportObject(m_totalMemory);
                p_exporter.exportObject(m_totalPayloadMemory);
                p_exporter.writeLong(m_numberOfActiveMemoryBlocks);
                p_exporter.writeLong(m_numberOfActiveChunks);
                p_exporter.exportObject(m_totalChunkPayloadMemory);
                p_exporter.writeLong(m_cidTableCount);
                p_exporter.exportObject(m_totalMemoryCIDTables);
                p_exporter.writeInt(m_cachedFreeLIDs);
                p_exporter.writeLong(m_availableFreeLIDs);
                p_exporter.writeLong(m_newLIDCounter);
            }

            @Override
            public void importObject(final Importer p_importer) {
                if (m_freeMemory == null) {
                    m_freeMemory = new StorageUnit();
                }
                p_importer.importObject(m_freeMemory);

                if (m_totalMemory == null) {
                    m_totalMemory = new StorageUnit();
                }
                p_importer.importObject(m_totalMemory);

                if (m_totalPayloadMemory == null) {
                    m_totalPayloadMemory = new StorageUnit();
                }
                p_importer.importObject(m_totalPayloadMemory);

                m_numberOfActiveMemoryBlocks = p_importer.readLong(m_numberOfActiveMemoryBlocks);
                m_numberOfActiveChunks = p_importer.readLong(m_numberOfActiveChunks);

                if (m_totalChunkPayloadMemory == null) {
                    m_totalChunkPayloadMemory = new StorageUnit();
                }
                p_importer.importObject(m_totalChunkPayloadMemory);

                m_cidTableCount = p_importer.readLong(m_cidTableCount);

                if (m_totalMemoryCIDTables == null) {
                    m_totalMemoryCIDTables = new StorageUnit();
                }
                p_importer.importObject(m_totalMemoryCIDTables);

                m_cachedFreeLIDs = p_importer.readInt(m_cachedFreeLIDs);
                m_availableFreeLIDs = p_importer.readLong(m_availableFreeLIDs);
                m_newLIDCounter = p_importer.readLong(m_newLIDCounter);
            }

            @Override
            public String toString() {
                String str = "";

                str += "Free memory: " + m_freeMemory.getHumanReadable() + " (" + m_freeMemory.getBytes() + ")\n";
                str += "Total memory: " + m_totalMemory.getHumanReadable() + " (" + m_totalMemory.getBytes() + ")\n";
                str += "Total payload memory: " + m_totalPayloadMemory.getHumanReadable() + " (" + m_totalPayloadMemory.getBytes() + ")\n";
                str += "Num active memory blocks: " + m_numberOfActiveMemoryBlocks + '\n';
                str += "Num active chunks: " + m_numberOfActiveChunks + '\n';
                str += "Total chunk payload memory: " + m_totalChunkPayloadMemory.getHumanReadable() + " (" + m_totalChunkPayloadMemory.getBytes() + ")\n";
                str += "Num CID tables: " + m_cidTableCount + '\n';
                str += "Total CID tables memory: " + m_totalMemoryCIDTables.getHumanReadable() + " (" + m_totalChunkPayloadMemory.getBytes() + ")\n";
                str += "Num of free LIDs cached in LIDStore: " + m_cachedFreeLIDs + '\n';
                str += "Num of total available free LIDs in LIDStore: " + m_availableFreeLIDs + '\n';
                str += "New LID counter state: " + m_newLIDCounter;
                return str;
            }
        }

    }
}
