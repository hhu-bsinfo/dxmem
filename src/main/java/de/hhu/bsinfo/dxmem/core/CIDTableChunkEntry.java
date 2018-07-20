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

// | pinned 1 bit | lock 8 bit (1 bit write, 7 bit read)
// | length field 12 bit (1 bit embedded/non embedded, 11 bits lf) | address 43 bit |
public final class CIDTableChunkEntry {
    static final long RAW_VALUE_FREE = 0;

    private static final long BITS_PINNED = 1;
    private static final long BITS_WRITE_LOCK = 1;
    private static final long BITS_READ_LOCK = 7;
    private static final long BITS_LENGTH_FIELD_IS_EMBEDDED = 1;
    private static final long BITS_EMBEDDED_LENGTH_FIELD = 11;
    private static final long BITS_ADDRESS = Address.WIDTH_BITS;

    private static final long MASK_PINNED = (long) Math.pow(2, BITS_PINNED) - 1;
    private static final long MASK_WRITE_LOCK = (long) Math.pow(2, BITS_WRITE_LOCK) - 1;
    private static final long MASK_READ_LOCK = (long) Math.pow(2, BITS_READ_LOCK) - 1;
    private static final long MASK_LENGTH_FIELD_IS_EMBEDDED = (long) Math.pow(2, BITS_LENGTH_FIELD_IS_EMBEDDED) - 1;
    private static final long MASK_EMBEDDED_LENGTH_FIELD = (long) Math.pow(2, BITS_EMBEDDED_LENGTH_FIELD) - 1;
    private static final long MASK_ADDRESS = (long) Math.pow(2, BITS_ADDRESS) - 1;

    static final long OFFSET_ADDRESS = 0;
    static final long OFFSET_EMBEDDED_LENGTH_FIELD = OFFSET_ADDRESS + BITS_ADDRESS;
    static final long OFFSET_LENGTH_FIELD_IS_EMBEDDED = OFFSET_EMBEDDED_LENGTH_FIELD + BITS_EMBEDDED_LENGTH_FIELD;
    static final long OFFSET_READ_LOCK = OFFSET_LENGTH_FIELD_IS_EMBEDDED + BITS_LENGTH_FIELD_IS_EMBEDDED;
    static final long OFFSET_WRITE_LOCK = OFFSET_READ_LOCK + BITS_READ_LOCK;
    static final long OFFSET_PINNED = OFFSET_WRITE_LOCK + BITS_WRITE_LOCK;

    // if split length field, split length data
    private static final long BITS_SPLIT_LENGTH_FIELD_SIZE = 2;
    // LSB stored in CIDTableChunkEntry, all other bytes with data in heap
    private static final long BITS_SPLIT_LENGTH_FIELD_LSB = 8;

    private static final long MASK_SPLIT_LENGTH_FIELD_SIZE = (long) Math.pow(2, BITS_SPLIT_LENGTH_FIELD_SIZE) - 1;
    static final long MASK_SPLIT_LENGTH_FIELD_LSB = (long) Math.pow(2, BITS_SPLIT_LENGTH_FIELD_LSB) - 1;

    static final long OFFSET_SPLIT_LENGTH_FIELD_SIZE = BITS_SPLIT_LENGTH_FIELD_LSB;
    static final long OFFSET_SPLIT_LENGTH_FIELD_LSB = 0;

    // raw pointer to the address where the entry is stored
    private long m_pointer;
    private long m_initialValue;

    private byte m_pinned;
    private byte m_writeLock;
    private byte m_readLock;
    private byte m_isLengthFieldEmbedded;

    private int m_embeddedLengthField;

    private int m_splitLengthFieldSize;
    private int m_splitLengthFieldLsb;
    private int m_splitLengthFieldMsb;

    private long m_address;

    public CIDTableChunkEntry() {

    }

    public CIDTableChunkEntry(final long p_pointer, final long p_value) {
        set(p_pointer, p_value);
    }

    public void clear() {
        m_pointer = Address.INVALID;
        m_initialValue = 0;

        m_pinned = 0;
        m_writeLock = 0;
        m_readLock = 0;
        m_isLengthFieldEmbedded = 0;

        m_embeddedLengthField = 0;

        m_splitLengthFieldSize = 0;
        m_splitLengthFieldLsb = 0;

        m_address = Address.INVALID;
    }

    public void set(final long p_pointer, final long p_value) {
        m_pointer = p_pointer;
        m_initialValue = p_value;

        m_pinned = (byte) (p_value >> OFFSET_PINNED & MASK_PINNED);
        m_writeLock = (byte) (p_value >> OFFSET_WRITE_LOCK & MASK_WRITE_LOCK);
        m_readLock = (byte) (p_value >> OFFSET_READ_LOCK & MASK_READ_LOCK);
        m_isLengthFieldEmbedded = (byte) (p_value >> OFFSET_LENGTH_FIELD_IS_EMBEDDED & MASK_LENGTH_FIELD_IS_EMBEDDED);

        if (m_isLengthFieldEmbedded > 0) {
            m_embeddedLengthField = (int) (p_value >> OFFSET_EMBEDDED_LENGTH_FIELD & MASK_EMBEDDED_LENGTH_FIELD);
            m_splitLengthFieldSize = 0;
            m_splitLengthFieldLsb = 0;
        } else {
            m_embeddedLengthField = 0;
            m_splitLengthFieldSize = (int) (p_value >> OFFSET_EMBEDDED_LENGTH_FIELD + OFFSET_SPLIT_LENGTH_FIELD_SIZE &
                    MASK_SPLIT_LENGTH_FIELD_SIZE);
            m_splitLengthFieldLsb = (int) (p_value >> OFFSET_EMBEDDED_LENGTH_FIELD + OFFSET_SPLIT_LENGTH_FIELD_LSB &
                    MASK_SPLIT_LENGTH_FIELD_LSB);
        }

        m_address = p_value >> OFFSET_ADDRESS & MASK_ADDRESS;
    }

    public void setPointer(final long p_pointer) {
        m_pointer = p_pointer;
    }

    public long getPointer() {
        return m_pointer;
    }

    public long getInitalValue() {
        return m_initialValue;
    }

    public long getValue() {
        long tmp = 0;

        tmp |= (m_pinned & MASK_PINNED) << OFFSET_PINNED;
        tmp |= (m_writeLock & MASK_WRITE_LOCK) << OFFSET_WRITE_LOCK;
        tmp |= (m_readLock & MASK_READ_LOCK) << OFFSET_READ_LOCK;

        tmp |= (m_isLengthFieldEmbedded & MASK_LENGTH_FIELD_IS_EMBEDDED) << OFFSET_LENGTH_FIELD_IS_EMBEDDED;

        if (m_isLengthFieldEmbedded > 0) {
            tmp |= (m_embeddedLengthField & MASK_EMBEDDED_LENGTH_FIELD) << OFFSET_EMBEDDED_LENGTH_FIELD;
        } else {
            tmp |= (m_splitLengthFieldSize & MASK_SPLIT_LENGTH_FIELD_SIZE) << OFFSET_EMBEDDED_LENGTH_FIELD +
                    OFFSET_SPLIT_LENGTH_FIELD_SIZE;
            tmp |= (m_splitLengthFieldLsb & MASK_SPLIT_LENGTH_FIELD_LSB) << OFFSET_EMBEDDED_LENGTH_FIELD +
                    OFFSET_SPLIT_LENGTH_FIELD_LSB;
        }

        tmp |= m_address << OFFSET_ADDRESS & MASK_ADDRESS;

        return tmp;
    }

    public boolean isValid() {
        return m_pointer != Address.INVALID && m_initialValue != RAW_VALUE_FREE &&
                m_initialValue != CIDTableZombieEntry.RAW_VALUE;
    }

    public boolean isPinned() {
        return m_pinned > 0;
    }

    public void setPinned(final boolean p_pinned) {
        m_pinned = (byte) (p_pinned ? 1 : 0);
    }

    public boolean isWriteLockAcquired() {
        return m_writeLock > 0;
    }

    public boolean acquireWriteLock() {
        if (m_writeLock > 0) {
            return false;
        } else {
            m_writeLock = 1;
            return true;
        }
    }

    public void releaseWriteLock() {
        assert m_writeLock > 0;

        m_writeLock = 0;
    }

    public boolean areReadLocksAcquired() {
        return m_readLock > 0;
    }

    public boolean acquireReadLock() {
        if (m_readLock == MASK_READ_LOCK) {
            return false;
        } else {
            m_readLock++;
            return true;
        }
    }

    public void releaseReadLock() {
        assert m_readLock > 0;

        m_readLock--;
    }

    public int getReadLockCounter() {
        return m_readLock;
    }

    public boolean isLengthFieldEmbedded() {
        return m_isLengthFieldEmbedded > 0;
    }

    public int getEmbeddedLengthField() {
        return m_embeddedLengthField;
    }

    public int getSplitLengthFieldLsb() {
        return m_splitLengthFieldLsb;
    }

    public int getSplitLengthFieldMsb() {
        return m_splitLengthFieldMsb;
    }

    public int getSplitLengthFieldSize() {
        return m_splitLengthFieldSize;
    }

    // sets the total length and calculates the embedded and split length field parts
    public void setLengthField(final int p_totalLength) {
        assert p_totalLength >= 0;

        if (p_totalLength > MASK_EMBEDDED_LENGTH_FIELD) {
            // split
            m_isLengthFieldEmbedded = 0;
            m_embeddedLengthField = 0;

            m_splitLengthFieldMsb = p_totalLength >> BITS_SPLIT_LENGTH_FIELD_LSB;
            m_splitLengthFieldLsb = (int) (p_totalLength & MASK_SPLIT_LENGTH_FIELD_LSB);

            m_splitLengthFieldSize = calculateMinStorageBytes(m_splitLengthFieldMsb);
        } else {
            // null split parts
            m_splitLengthFieldLsb = 0;
            m_splitLengthFieldSize = 0;

            // embedded
            m_isLengthFieldEmbedded = 1;
            m_embeddedLengthField = p_totalLength;
        }
    }

    public int combineWithSplitLengthFieldData(final int p_splitLengthFieldData) {
        return p_splitLengthFieldData << BITS_SPLIT_LENGTH_FIELD_LSB | m_splitLengthFieldLsb;
    }

    public boolean isAddressValid() {
        return m_address != Address.INVALID;
    }

    public long getAddress() {
        return m_address;
    }

    public void setAddress(final long p_value) {
        assert p_value >= 0 && p_value <= MASK_ADDRESS;

        m_address = p_value;
    }

    @Override
    public String toString() {
        return "m_pointer " + Address.toHexString(m_pointer) + ", m_initialValue " +
                Address.toHexString(m_initialValue) + ", m_pinned " + m_pinned + ", m_writeLock " + m_writeLock +
                ", m_readLock " + m_readLock + ", m_isLengthFieldEmbedded " + m_isLengthFieldEmbedded +
                ", m_embeddedLengthField " + m_embeddedLengthField + ", m_splitLengthFieldSize " +
                m_splitLengthFieldSize + ", m_splitLengthFieldLsb " + m_splitLengthFieldLsb +
                ", m_splitLengthFieldMsb " + m_splitLengthFieldMsb + ", m_address " + Address.toHexString(m_address);
    }

    public static int calculateLengthFieldSizeHeapBlock(final int p_totalSize) {
        if (p_totalSize <= MASK_EMBEDDED_LENGTH_FIELD) {
            return 0;
        } else {
            return calculateMinStorageBytes(p_totalSize >> BITS_SPLIT_LENGTH_FIELD_LSB);
        }
    }

    private static int calculateMinStorageBytes(final int p_val) {
        int n = 0;
        int val = p_val;

        while (val != 0) {
            val >>= 8;
            n++;
        }

        return n;
    }

    public static long getAddressOfRawEntry(final long p_rawValue) {
        return p_rawValue >> OFFSET_ADDRESS & MASK_ADDRESS;
    }
}
