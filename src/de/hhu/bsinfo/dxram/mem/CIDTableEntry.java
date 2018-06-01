package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxutils.BitMask;

/**
 * Central place for the CIDTable entry logic
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 16.02.18
 * @projectname dxram-memory
 */
public class CIDTableEntry {
    //Flags vor level 0 entries
    static final int S_NORMAL = 0;
    static final int S_NOT_MOVE = 1;
    static final int S_NOT_REMOVE = 2;

    //43 Bit: the address size of a chunk
    static final EntryData ADDRESS = EntryData.create(43);

    //1 Bit: Object is bigger than 2^10
    static final EntryBit EMBEDDED_LENGTH_FIELD = EntryBit.create(1);

    //2 Bit: If object is bigger 2^10 save size of the length field
    static final EntryData LENGTH_FIELD_SIZE = EntryData.create(2);

    //8 Bit: as external length field if object is bigger 2^10
    static final EntryData PARTED_LENGTH_FIELD = EntryData.create(8);

    //If object is smaller or equal 2^10 then save no length field.
    static final EntryData LENGTH_FIELD = EntryData.combineData(LENGTH_FIELD_SIZE, PARTED_LENGTH_FIELD);

    // 7 Bit: Count the parallel read access
    static final EntryData READ_ACCESS = EntryData.create(7);
    static final long READ_INCREMENT = 1L << READ_ACCESS.OFFSET;

    // 1 Bit: Mark a wanted write access
    static final EntryBit WRITE_ACCESS = EntryBit.create(1);

    // 1 Bit: no remove allowed (e.g. to purpose a fast path)
    static final EntryBit STATE_NOT_REMOVEABLE = EntryBit.create(1);

    // 1 Bit: no move allowed (e.g. to purpose defragmentation)
    static final EntryBit STATE_NOT_MOVEABLE = EntryBit.create(1);

    //not moveable implies not removeable so we can use this combination for a full list or a unused cid
    static final EntryBit FULL_FLAG = EntryBit.combineData(STATE_NOT_MOVEABLE, STATE_NOT_REMOVEABLE);

    /**
     * Create a normal level 0 entry
     *
     * @param p_address
     *          Address on the heap
     * @param p_size
     *          Size of the chunk
     * @param p_lengthFieldSize
     *          The length field size of the block
     * @return A generated normal entry for the CID Table
     */
    static long createEntry(final long p_address, final int p_size, final int p_lengthFieldSize){
        return createEntry(p_address, p_size, p_lengthFieldSize, S_NORMAL);
    }

    /**
     * Create a level 0 entry
     *
     * @param p_addressChunk
     *          Address on the heap
     * @param p_size
     *          Size of the chunk
     * @param p_lengthFieldSize
     *          The length field size of the block
     * @param p_state
     *          State of the Chunk use S_NORMAL, S_NOT_MOVEABLE or S_NOT_REMOVEABLE
     * @return A generated entry for the CID Table
     */
    private static long createEntry(final long p_addressChunk, final int p_size, final int p_lengthFieldSize, int p_state){
        long entry;
        //int lfs = SmallObjectHeap.calculateLengthFieldSizeAllocBlock(p_size);

        entry = ADDRESS.set(0, p_addressChunk+p_lengthFieldSize);
        if(p_lengthFieldSize != 0){
            entry = EMBEDDED_LENGTH_FIELD.set(entry, true);
            entry = PARTED_LENGTH_FIELD.set(entry, p_size-1);
            entry = LENGTH_FIELD_SIZE.set(entry, p_lengthFieldSize);
        } else{
            entry = LENGTH_FIELD.set(entry, p_size-1);
        }
        if(p_state != S_NORMAL){
            entry = STATE_NOT_REMOVEABLE.set(entry, p_state == S_NOT_REMOVE);
            entry = STATE_NOT_MOVEABLE.set(entry, p_state == S_NOT_MOVE);
        }

        return entry;
    }

    /**
     * Debugging: Get a formatted string from a level 0 entry
     *
     * @param p_entry
     *          the entry data
     * @return
     *          a String with detailed information about the chunk
     */
    static String entryData(final long p_entry){
        return String.format("address: 0x%X, lf: %d, read: %d, write: %b, moveable: %b, removeable: %b, full: %b",
                ADDRESS.get(p_entry),
                LENGTH_FIELD.get(p_entry),
                READ_ACCESS.get(p_entry),
                WRITE_ACCESS.get(p_entry),
                !STATE_NOT_MOVEABLE.get(p_entry),
                !STATE_NOT_REMOVEABLE.get(p_entry),
                FULL_FLAG.get(p_entry));

    }

    /**
     * Handle bit masks and data offset for level 0 entries
     */
    private static class Entry {
        private static BitMask bm = new BitMask(Long.SIZE);

        public final long BITMASK;
        public final byte OFFSET;
        public final byte SIZE;

        /**
         * Constructor
         *
         * @param neededBits Needed bit for the entry
         */
        private Entry(final byte neededBits){
            OFFSET = bm.getUsedBits();
            BITMASK = bm.checkedCreate(neededBits);
            SIZE = neededBits;
        }

        private Entry(Entry e1, Entry e2){
            OFFSET = (byte) Math.min(e1.OFFSET, e2.OFFSET);
            BITMASK = e1.BITMASK | e2.BITMASK;
            SIZE = (byte) (e1.SIZE + e2.SIZE);
        }

    }

    public static final class EntryData extends Entry {

        private EntryData(final byte neededBytes){
            super(neededBytes);
        }

        private EntryData(Entry e1, Entry e2){
            super(e1, e2);
        }

        private static EntryData create(final int neededBits){
            return new EntryData((byte) neededBits);
        }

        private static EntryData combineData(Entry e1, Entry e2){
            return new EntryData(e1, e2);
        }

        /**
         * Get the saved data from a entry
         *
         * @param p_tableEntry the level 0 table entry
         * @return the saved data
         */
        public final long get(final long p_tableEntry){
            return (p_tableEntry & BITMASK) >> OFFSET;
        }

        public final long set(final long p_tableEntry, final long value){
            return (p_tableEntry & ~BITMASK) | ((value << OFFSET) & BITMASK);
        }
    }

    public static final class EntryBit extends Entry {

        /**
         * Constructor
         *
         * @param neededBits Needed bit for the entry
         */
        private EntryBit(byte neededBits) {
            super(neededBits);
        }

        private EntryBit(Entry e1, Entry e2) {
            super(e1, e2);
        }

        private static EntryBit create(final int neededBits){
            return new EntryBit((byte) neededBits);
        }

        private static EntryBit combineData(Entry e1, Entry e2){
            return new EntryBit(e1, e2);
        }

        /**
         * Get the saved data from a entry
         *
         * @param p_tableEntry the level 0 table entry
         * @return the saved data
         */
        public final boolean get(final long p_tableEntry){
            return (p_tableEntry & BITMASK) == BITMASK;
        }

        public final long set(final long p_tableEntry, final boolean value){
            return (value) ? (p_tableEntry | BITMASK):(p_tableEntry & ~BITMASK);
        }

    }
}
