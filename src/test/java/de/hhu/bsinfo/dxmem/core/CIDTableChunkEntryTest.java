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

import org.junit.Assert;
import org.junit.Test;

public class CIDTableChunkEntryTest {
    private long m_pointer = 0xAABBCCDDEEL;
    private long m_address = 0x11223344L;
    private boolean m_pinned = true;
    private int m_size = 2047;
    private int m_size2 = 2048;
    private long m_value = m_address << CIDTableChunkEntry.OFFSET_ADDRESS |
            (long) m_size << CIDTableChunkEntry.OFFSET_EMBEDDED_LENGTH_FIELD |
            1L << CIDTableChunkEntry.OFFSET_LENGTH_FIELD_IS_EMBEDDED |
            1L << CIDTableChunkEntry.OFFSET_READ_LOCK |
            1L << CIDTableChunkEntry.OFFSET_WRITE_LOCK |
            (long) (m_pinned ? 1 : 0) << CIDTableChunkEntry.OFFSET_PINNED;

    private long m_value2 =
            (long) (m_pinned ? 1 : 0) << CIDTableChunkEntry.OFFSET_PINNED |
                    1L << CIDTableChunkEntry.OFFSET_WRITE_LOCK |
                    1L << CIDTableChunkEntry.OFFSET_READ_LOCK |
                    0L << CIDTableChunkEntry.OFFSET_LENGTH_FIELD_IS_EMBEDDED |
                    (long) CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(m_size2) <<
                            CIDTableChunkEntry.OFFSET_EMBEDDED_LENGTH_FIELD +
                                    CIDTableChunkEntry.OFFSET_SPLIT_LENGTH_FIELD_SIZE |
                    m_size2 & CIDTableChunkEntry.MASK_SPLIT_LENGTH_FIELD_LSB <<
                            CIDTableChunkEntry.OFFSET_EMBEDDED_LENGTH_FIELD +
                                    CIDTableChunkEntry.OFFSET_SPLIT_LENGTH_FIELD_LSB |
                    m_address << CIDTableChunkEntry.OFFSET_ADDRESS;

    @Test
    public void embeddedLengthField() {
        CIDTableChunkEntry entry1 = new CIDTableChunkEntry();
        CIDTableChunkEntry entry2 = new CIDTableChunkEntry();

        Assert.assertEquals(false, entry1.isValid());
        Assert.assertEquals(false, entry1.isAddressValid());
        Assert.assertEquals(false, entry1.areReadLocksAcquired());
        Assert.assertEquals(false, entry1.isWriteLockAcquired());

        entry1.setPinned(m_pinned);
        Assert.assertTrue(entry1.acquireReadLock());
        Assert.assertTrue(entry1.acquireWriteLock());
        entry1.setLengthField(m_size);
        entry1.setAddress(m_address);
        entry1.setPointer(m_pointer);

        Assert.assertEquals(m_address, entry1.getAddress());
        Assert.assertEquals(m_pinned, entry1.isPinned());
        Assert.assertEquals(true, entry1.isLengthFieldEmbedded());
        Assert.assertEquals(true, entry1.isAddressValid());
        Assert.assertEquals(true, entry1.areReadLocksAcquired());
        Assert.assertEquals(true, entry1.isWriteLockAcquired());
        Assert.assertEquals(m_size, entry1.getEmbeddedLengthField());
        Assert.assertEquals(0, entry1.getSplitLengthFieldSize());
        Assert.assertEquals(m_pointer, entry1.getPointer());

        entry2.set(m_pointer, m_value);
        Assert.assertEquals(true, entry2.isValid());

        Assert.assertEquals(entry2.getValue(), entry1.getValue());
        Assert.assertEquals(entry2.getAddress(), entry1.getAddress());
        Assert.assertEquals(entry2.getPointer(), entry1.getPointer());
    }

    @Test
    public void splitLengthField() {
        CIDTableChunkEntry entry1 = new CIDTableChunkEntry();
        CIDTableChunkEntry entry2 = new CIDTableChunkEntry();

        Assert.assertEquals(false, entry1.isValid());
        Assert.assertEquals(false, entry1.isAddressValid());
        Assert.assertEquals(false, entry1.areReadLocksAcquired());
        Assert.assertEquals(false, entry1.isWriteLockAcquired());

        entry1.setPinned(true);
        Assert.assertTrue(entry1.acquireReadLock());
        Assert.assertTrue(entry1.acquireWriteLock());
        entry1.setLengthField(m_size2);
        entry1.setAddress(m_address);
        entry1.setPointer(m_pointer);

        Assert.assertEquals(m_address, entry1.getAddress());
        Assert.assertEquals(m_pinned, entry1.isPinned());
        Assert.assertEquals(false, entry1.isLengthFieldEmbedded());
        Assert.assertEquals(true, entry1.isAddressValid());
        Assert.assertEquals(true, entry1.areReadLocksAcquired());
        Assert.assertEquals(true, entry1.isWriteLockAcquired());
        Assert.assertEquals(0, entry1.getEmbeddedLengthField());
        Assert.assertEquals(m_size2, entry1.combineWithSplitLengthFieldData(entry1.getSplitLengthFieldMsb()));
        Assert.assertEquals(1, entry1.getSplitLengthFieldSize());
        Assert.assertEquals(m_pointer, entry1.getPointer());

        entry2.set(m_pointer, m_value2);
        Assert.assertEquals(true, entry2.isValid());

        Assert.assertEquals(entry2.getValue(), entry1.getValue());
        Assert.assertEquals(entry2.getAddress(), entry1.getAddress());
        Assert.assertEquals(entry2.getPointer(), entry1.getPointer());
    }

    @Test
    public void staticHelpers() {
        Assert.assertEquals(m_address, CIDTableChunkEntry.getAddressOfRawEntry(m_value));

        Assert.assertEquals(0, CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(0xFF));
        Assert.assertEquals(0, CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(0x100));
        Assert.assertEquals(0, CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(0x7FF));
        Assert.assertEquals(1, CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(0x800));
        Assert.assertEquals(2, CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(0x3FFFF));
        Assert.assertEquals(3, CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(0x1000000));

        Assert.assertEquals(0, CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(m_size));
        Assert.assertEquals(1, CIDTableChunkEntry.calculateLengthFieldSizeHeapBlock(m_size2));
    }
}
