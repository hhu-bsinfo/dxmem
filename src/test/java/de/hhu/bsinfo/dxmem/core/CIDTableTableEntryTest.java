package de.hhu.bsinfo.dxmem.core;

import org.junit.Assert;
import org.junit.Test;

public class CIDTableTableEntryTest {
    private long m_pointer = 0x198374621L;
    private long m_address = 0xAA723BBCCL;
    private long m_value = m_address << CIDTableTableEntry.OFFSET_ADDRESS;

    @Test
    public void test() {
        CIDTableTableEntry entry1 = new CIDTableTableEntry();
        CIDTableTableEntry entry2 = new CIDTableTableEntry();

        Assert.assertEquals(false, entry1.isAddressValid());

        entry1.setAddress(m_address);
        entry1.setPointer(m_pointer);

        Assert.assertEquals(m_address, entry1.getAddress());

        Assert.assertEquals(true, entry1.isAddressValid());
        Assert.assertEquals(m_pointer, entry1.getPointer());

        entry2.set(m_pointer, m_value);

        Assert.assertEquals(entry2.getValue(), entry1.getValue());
        Assert.assertEquals(entry2.getAddress(), entry1.getAddress());
        Assert.assertEquals(entry2.getPointer(), entry1.getPointer());
    }
}
