package de.hhu.bsinfo.dxmem.core;

import org.junit.Assert;
import org.junit.Test;

public class CIDTableTableEntryTest {
    private long m_pointer = 0x198374621L;
    private long m_address = 0xAA723BBCCL;
    private int m_numEntries = 743;
    private long m_value = m_address << CIDTableTableEntry.OFFSET_ADDRESS |
            (long) m_numEntries << CIDTableTableEntry.OFFSET_USED_ENTRIES_NEXT_TABLE |
            1L << CIDTableTableEntry.OFFSET_READ_LOCK |
            1L << CIDTableTableEntry.OFFSET_WRITE_LOCK;

    @Test
    public void test() {
        CIDTableTableEntry entry1 = new CIDTableTableEntry();
        CIDTableTableEntry entry2 = new CIDTableTableEntry();

        Assert.assertEquals(false, entry1.isAddressValid());
        Assert.assertEquals(false, entry1.areReadLocksAcquired());
        Assert.assertEquals(false, entry1.isWriteLockAcquired());

        Assert.assertTrue(entry1.acquireReadLock());
        Assert.assertTrue(entry1.acquireWriteLock());
        entry1.setNumEntriesUsedNextTable(m_numEntries);
        entry1.setAddress(m_address);
        entry1.setPointer(m_pointer);

        Assert.assertEquals(m_address, entry1.getAddress());

        Assert.assertEquals(true, entry1.isAddressValid());
        Assert.assertEquals(true, entry1.areReadLocksAcquired());
        Assert.assertEquals(true, entry1.isWriteLockAcquired());
        Assert.assertEquals(m_numEntries, entry1.getNumEntriesUsedNextTable());
        Assert.assertEquals(m_pointer, entry1.getPointer());

        entry2.set(m_pointer, m_value);

        Assert.assertEquals(entry2.getValue(), entry1.getValue());
        Assert.assertEquals(entry2.getAddress(), entry1.getAddress());
        Assert.assertEquals(entry2.getPointer(), entry1.getPointer());
    }
}
