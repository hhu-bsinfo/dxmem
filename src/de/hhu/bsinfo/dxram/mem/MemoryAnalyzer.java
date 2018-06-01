package de.hhu.bsinfo.dxram.mem;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

import static de.hhu.bsinfo.dxram.mem.CIDTable.*;
import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.*;
import static de.hhu.bsinfo.dxram.mem.SmallObjectHeap.*;

/**
 * Analyze and print the structure of the MemoryComponent
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 21.02.18
 * @projectname dxram-memory
 */
@SuppressWarnings({"SameParameterValue", "FieldCanBeLocal"})
final public class MemoryAnalyzer {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MemoryAnalyzer.class.getSimpleName());

    private final SmallObjectHeap smallObjectHeap;
    private final CIDTable cidTable;
    private final MemoryManager.MemoryInformation info;
    private boolean m_quiet;
    private boolean m_dumpOnError;

    private LinkedList<Long> m_freeBlocks;
    private long nextFree;

    private LinkedList<Long> m_dataBlocks;
    private long nextData;

    private LinkedList<Long> m_managementTables;
    private long nextTable;

    private long predictedAddress = SIZE_MARKER_BYTE;
    private boolean ERROR = false;


    /**
     * Constructor
     *
     * @param memoryManager The central unit which manages all memory accesses
     */
    MemoryAnalyzer(final MemoryManager memoryManager){
        cidTable = memoryManager.cidTable;
        smallObjectHeap = memoryManager.smallObjectHeap;
        info = memoryManager.info;

    }

    /**
     * Analyze the memory structure
     *
     * @param p_quiet Be quiet. If true print only the first occurring error
     * @return True if no error occurred, false else
     */
    final public boolean analyze(final boolean p_quiet) {
        return analyze(p_quiet, false);
    }

    /**
     * Analyze the memory structure
     *
     * @param p_quiet Be quiet. If true print only the first occurring error
     * @param p_dumpOnError Create a heap dump on a Error
     * @return True if no error occurred, false else
     */
    final public boolean analyze(final boolean p_quiet, final boolean p_dumpOnError){
        assert  cidTable.getNextLocalIDCounter()-1 < Integer.MAX_VALUE;
        m_quiet = p_quiet;
        m_dumpOnError = p_dumpOnError;

        long address;

        //collect data
        collectData();


        //Get free blocks from the free block lists and sort them
        nextFree = getNext(m_freeBlocks);

        //Get the entries from the level 0 tables and sort them by the address
        nextData = getNext(m_dataBlocks);

        //Get the tables(NID Table and the tables for level 4 to 1) them by the address
        nextTable = getNext(m_managementTables);

        while (nextFree < Long.MAX_VALUE || nextData < Long.MAX_VALUE || nextTable < Long.MAX_VALUE) {

            //read the marker
            int marker = smallObjectHeap.readRightPartOfMarker(predictedAddress - SIZE_MARKER_BYTE);

            switch (marker) {
                case 0:
                    checkFreeBlock(predictedAddress, false);
                    break;
                case 1:
                case 2:
                    if (predictedAddress != nextFree) {
                        ERROR = true;
                        LOGGER.error(String.format("1\033[0;31m >> expected: 0x%X get: 0x%X\033[0m", predictedAddress, nextFree));
                    }

                    checkFreeBlock(predictedAddress, true);
                    nextFree = getNext(m_freeBlocks);
                    break;
                case 3:
                    if (ADDRESS.get(nextData) < ADDRESS.get(nextTable)) {
                        //block have to be a data block
                        address = ADDRESS.get(nextData);
                        if (predictedAddress != address) {
                            ERROR = true;
                            LOGGER.error(String.format("2\033[0;31m >> expected: 0x%X get: 0x%X\033[0m", predictedAddress, address));
                        }
                        checkDataBlock(nextData);
                        nextData = getNext(m_dataBlocks);
                    } else {
                        //block is a table
                        address = ADDRESS.get(nextTable);
                        if (predictedAddress != address) {
                            ERROR = true;
                            LOGGER.error(String.format("3\033[0;31m >> expected: 0x%X get: 0x%X\033[0m", predictedAddress, address));
                        }
                        checkTable(address);
                        nextTable = getNext(m_managementTables);
                    }
                    break;
                case 4:
                case 5:
                case 6:
                case 7:
                    address = ADDRESS.get(nextData);
                    if(EMBEDDED_LENGTH_FIELD.get(nextData))
                        address -= LENGTH_FIELD_SIZE.get(nextData);

                    if (predictedAddress != address) {
                        ERROR = true;
                        LOGGER.error(String.format("4\033[0;31m >> expected: 0x%X get: 0x%X\033[0m", predictedAddress, address));
                    }
                    checkDataBlock(nextData);
                    nextData = getNext(m_dataBlocks);
                    break;
                case SINGLE_BYTE_MARKER:
                    checkFreeBlock(predictedAddress, false);
                    break;

            }

            if (ERROR || predictedAddress == smallObjectHeap.m_baseFreeBlockList) {
                break;
            }
        }

        if(p_dumpOnError && ERROR)
            dump("./heap.dump");

        if (!ERROR)
            LOGGER.info("Checked all: \033[0;32mNO ERRORS\033[0m");

        return !ERROR;

    }

    /**
     * Collect all Heap data
     */
    private void collectData() {
        LOGGER.info(String.format("Init with: be quiet(print only errors): %b, do dump on error: %b", m_quiet, m_dumpOnError));

        //free blocks are managed with lists. They fully describe them self
        m_freeBlocks = getAllFreeBlocks();
        Collections.sort(m_freeBlocks);

        //dataBlock have a divided length field in cid 10 bits and 0-32 bits in the data block
        m_dataBlocks = getAllDataBlocks();
        m_dataBlocks.sort(Comparator.comparingLong(CIDTableEntry.ADDRESS::get));

        //tables have no length fields because the size is fixed
        m_managementTables = getAllManagementTables();
        m_managementTables.sort(Comparator.comparingLong(CIDTableEntry.ADDRESS::get));

        LOGGER.info(String.format("Collected data. Found: managed free blocks: %d, data blocks: %d, table: %d",
                m_freeBlocks.size(), m_dataBlocks.size(), m_managementTables.size()));
    }


    //Get the lists

    /**
     * Get a LinkedList of all managed free blocks
     *
     * @return LinkedList with all managed free blocks
     */
    private LinkedList<Long> getAllFreeBlocks(){
        LinkedList<Long> list = new LinkedList<>();
        int lfs;

        long tmp_address;

        //iterate over all free block lists
        for (long i = smallObjectHeap.m_baseFreeBlockList; i < smallObjectHeap.getStatus().getSize(); i+=POINTER_SIZE) {
            tmp_address = smallObjectHeap.read(i, POINTER_SIZE);

            //iterate over all entries in the current list
            while (tmp_address != INVALID_ADDRESS){
                list.add(tmp_address);

                //next element from list
                lfs = getSizeFromMarker(smallObjectHeap.readRightPartOfMarker(tmp_address-SIZE_MARKER_BYTE));
                tmp_address = smallObjectHeap.read(tmp_address + lfs + POINTER_SIZE, POINTER_SIZE);
            }
        }

        return list;
    }

    /**
     * Get a LinkedList of all active not migrated level0 entries
     *
     * @return LinkedList with all active not migrated level0 entries
     */
    private LinkedList<Long> getAllDataBlocks(){
        long found = 0;
        int counter = 0;
        LinkedList<Long> list = new LinkedList<>();
        long entry;

        //iterate over all possible chunk ids
        while (found < info.numActiveChunks){
            entry = cidTable.get(counter++);

            if ( entry == FREE_ENTRY || entry == ZOMBIE_ENTRY ){
                continue;
            }

            found++;
            list.add(entry);
        }
        return list;
    }

    /**
     * Get a LinkedList with all tables saved in the local SmallObjectHeap
     *
     * @return LinkedList with all tables (NID and level 4 to 1 )
     */
    private LinkedList<Long> getAllManagementTables(){
        LinkedList<Long> tables = new LinkedList<>();
        tables.add(cidTable.getAddressTableDirectory());

        int level = LID_TABLE_LEVELS;
        int counter = 0;
        long curTable;
        long entry;
        long tableEntries = ENTRIES_FOR_NID_LEVEL;
        long tableSize = NID_TABLE_SIZE;


        //iterate over all level bigger than level 0
        while(level > 0){
            try {
                if(level != LID_TABLE_LEVELS){
                    tableEntries = ENTRIES_PER_LID_LEVEL;
                    tableSize = LID_TABLE_SIZE;
                }
                curTable = ADDRESS.get(tables.get(counter++));

                //iterate over all table entries
                for (int i = 0; i < tableEntries; i++) {
                    entry = cidTable.readEntry(curTable, i);
                    if( entry == 0 || entry == ZOMBIE_ENTRY) {
                        continue;
                    }
                    tables.addLast(entry);
                }

                level--;

            } catch (IndexOutOfBoundsException e){
                break;
            }
        }

        return tables;
    }

    /**
     * Get next element of a list and poll it
     * 
     * @param list 
     *          List to work on
     * @return A list element of Long.MAX_VALUE if list is empty
     */
    private static long getNext(final LinkedList<Long> list){
        Long ret = list.pollFirst();

        return (ret != null) ? ret:Long.MAX_VALUE;
    }

    /**
     * Check a data block
     *
     * @param cidEntry Entry in the CIDTable(level 0)
     */
    private void checkDataBlock(final long cidEntry){
        String cidInfo = entryData(cidEntry);
        StringBuilder out = new StringBuilder("\033[0;33m[DATA]\033[0m\tCID Entry: [" + cidInfo + "], Heap: [");

        int lfs;
        if(EMBEDDED_LENGTH_FIELD.get(cidEntry)){
            lfs = (int)LENGTH_FIELD_SIZE.get(cidEntry);
        } else {
            lfs = 0;
        }

        long address = ADDRESS.get(cidEntry) - lfs;

        out.append(createLog(INVALID_ADDRESS < address && address < smallObjectHeap.m_baseFreeBlockList-SIZE_MARKER_BYTE,
                String.format("address: 0x%012X, ", address)));

        int marker = smallObjectHeap.readRightPartOfMarker(address-SIZE_MARKER_BYTE);
        out.append(createLog(ALLOC_BLOCK_FLAGS_OFFSET <= marker && marker <= 0x7, String.format("marker: %d, ", marker)));

        int lfsFromMarker = getSizeFromMarker(marker);
        out.append(createLog(0 <= lfsFromMarker && lfsFromMarker <= 4, String.format("lfs: %d, ", lfs)));
        out.append(createLog(lfs == lfsFromMarker, String.format("lfs: [m: %d, cid: %d], ", lfsFromMarker, lfs)));

        long lf = smallObjectHeap.read(address, lfs);
        out.append(createLog(0 <= lf && lf < (long)Math.pow(2,32), String.format("internal lf: %d", lf )));

        long fullLF = smallObjectHeap.getSizeDataBlock(cidEntry);
        out.append(createLog(1 <= fullLF && fullLF <= (long)Math.pow(2,42), String.format("], combined lf: %d", fullLF )));

        if(marker != smallObjectHeap.readLeftPartOfMarker(address + lfs + fullLF)){
            ERROR = true;
            out.append("\033[0;31mERROR->\033[0m").append(">>marker differ<<");
        }

        if(!ERROR)
            predictedAddress = address + lfs + fullLF + SIZE_MARKER_BYTE;

        if(ERROR)
            LOGGER.error(out.toString());
        else if (!m_quiet)
            LOGGER.info(out.toString());

    }

    /**
     * Check a free block
     *
     * @param p_address Address of the fre block
     * @param p_managed Is the free block hooked i a free block list?
     */
    private void checkFreeBlock(final long p_address, final boolean p_managed){
        StringBuilder out = new StringBuilder("\033[0;34m[FREE]\033[0m\t");

        out.append(createLog(INVALID_ADDRESS < p_address && p_address < smallObjectHeap.m_baseFreeBlockList-SIZE_MARKER_BYTE,
                String.format("address: 0x%X, ", p_address)));

        int marker = smallObjectHeap.readRightPartOfMarker(p_address -SIZE_MARKER_BYTE);
        out.append(createLog(0 <= marker && marker < ALLOC_BLOCK_FLAGS_OFFSET || marker == SINGLE_BYTE_MARKER,
                String.format("marker: %d, ", marker)));


        int lfs = getSizeFromMarker(marker);
        out.append(createLog(lfs == 0 || lfs == 1 || lfs == POINTER_SIZE, String.format("lfs: %d, ", lfs)));

        long lf = 0;
        if(marker != SINGLE_BYTE_MARKER) {
            lf = smallObjectHeap.read(p_address, lfs);
            out.append(createLog(lf <= smallObjectHeap.getStatus().getFree() || lf == 0, String.format("length field: %d, ", lf)));

            if (lf != smallObjectHeap.read(p_address + lf - lfs, lfs)) {
                ERROR = true;
                out.append(String.format("\033[0;31m[ERROR]>>length field differ [l: %d, r: %d] <<\033[0m, ",
                        lf, smallObjectHeap.read(p_address + lf - lfs, lfs)));
            }
        }

        if (marker != smallObjectHeap.readLeftPartOfMarker(p_address + lf)){
            ERROR = true;
            out.append(String.format("\033[0;31m[ERROR]>>marker differ [l: %d, r: %d]<<\033[0m, ",
                    marker, smallObjectHeap.readLeftPartOfMarker(p_address +lf)));
        }

        if(p_managed) {
            long pre = smallObjectHeap.readPointer(p_address + lfs);
            out.append(createLog(INVALID_ADDRESS < pre && pre < smallObjectHeap.m_baseFreeBlockList - SIZE_MARKER_BYTE ||
                    (pre - smallObjectHeap.m_baseFreeBlockList) % POINTER_SIZE == 0, String.format("pre: 0x%012X, ", pre)));

            long next = smallObjectHeap.readPointer(p_address + lfs + POINTER_SIZE);
            out.append(createLog(INVALID_ADDRESS <= next &&
                    p_address < smallObjectHeap.m_baseFreeBlockList - SIZE_MARKER_BYTE,
                    String.format("next: 0x%012X, ", next)));
        } else {
            out.append("not managed free block");
        }

        if(!ERROR)
            predictedAddress = p_address + lf + SIZE_MARKER_BYTE;

        if(ERROR)
            LOGGER.error(out.toString());
        else if (!m_quiet)
            LOGGER.info(out.toString());

    }

    /**
     * Get info about a table of the CIDTables
     *
     * @param p_tableAddress Address of the Table
     */
    private void checkTable(final long p_tableAddress){
        StringBuilder out = new StringBuilder();
        long numberEntries;
        long sizeTable;
        long entry;
        int freeEntries = 0;
        int fullEntries = 0;
        int zombieEntries = 0;

        if(p_tableAddress == cidTable.getAddressTableDirectory()){
            out.append("\033[0;35m[NID]\033[0m\t");
            numberEntries = ENTRIES_FOR_NID_LEVEL;
            sizeTable = NID_TABLE_SIZE;
        } else {
            out.append("\033[0;36m[LID]\033[0m\t");
            numberEntries = ENTRIES_PER_LID_LEVEL;
            sizeTable = LID_TABLE_SIZE;
        }

        out.append(String.format("address: 0x%X, ", p_tableAddress));

        int countActive = 0;
        for (int i = 0; i < numberEntries; i++) {
            entry = cidTable.readEntry(p_tableAddress, i);

            if(entry == 0) freeEntries++;
            else if(entry == ZOMBIE_ENTRY) zombieEntries++;
            else if(FULL_FLAG.get(entry)) fullEntries++;
            else countActive++;
        }

        if(p_tableAddress == cidTable.getAddressTableDirectory()){
            out.append(String.format("active slots: %d, free slots: %d", countActive, freeEntries));

        } else {
            out.append(String.format("active slots: %d, free slots: %d, zombie slots: %d, full slots: %d",
                    countActive, freeEntries, zombieEntries, fullEntries));
        }

        if(!ERROR)
            predictedAddress = p_tableAddress + sizeTable + SIZE_MARKER_BYTE;

        if(ERROR)
            LOGGER.error(out.toString());
        else if (!m_quiet)
            LOGGER.info(out.toString());

    }

    /**
     * dump the list and write heap dump to a given path
     *
     * @param path A path for a heap dump
     */
    private void dump(final String path){
        System.out.println("Free Blocks: [next: " + nextFree + "], " + Arrays.toString(m_freeBlocks.toArray()));
        System.out.println("Data Blocks: [next: " + nextData + "], " + Arrays.toString(m_dataBlocks.toArray()));
        System.out.println("Tables: [next: " + nextFree + "], " + Arrays.toString(m_managementTables.toArray()));

        smallObjectHeap.dump(path);
    }

    /**
     * Create a conditional log entry part
     *
     * @param p_condition Condition for the entry (true everything like expected, false else)
     * @param p_msg The message
     *
     */
    private String createLog(final boolean p_condition, final String p_msg){
        if(p_condition){
            //good
            return p_msg;
        } else {
            //bad
            ERROR = true;
            return "\033[0;31mERROR->\033[0m" + p_msg;
        }
    }
}
