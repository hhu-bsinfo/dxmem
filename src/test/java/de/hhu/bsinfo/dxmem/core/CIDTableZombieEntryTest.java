package de.hhu.bsinfo.dxmem.core;

import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.data.ChunkID;

public class CIDTableZombieEntryTest {
    private long m_pointer = 0x198374621L;
    private long m_cid = 0x12341234124L;

    @Test
    public void test() {
        CIDTableZombieEntry entry1 = new CIDTableZombieEntry();
        Assert.assertEquals(Address.INVALID, entry1.getPointer());
        Assert.assertEquals(ChunkID.INVALID_ID, entry1.getCID());

        entry1.set(m_pointer, m_cid);
        Assert.assertEquals(m_pointer, entry1.getPointer());
        Assert.assertEquals(m_cid, entry1.getCID());

        entry1.clear();
        Assert.assertEquals(Address.INVALID, entry1.getPointer());
        Assert.assertEquals(ChunkID.INVALID_ID, entry1.getCID());

        entry1 = new CIDTableZombieEntry(m_pointer, m_cid);
        Assert.assertEquals(m_pointer, entry1.getPointer());
        Assert.assertEquals(m_cid, entry1.getCID());
    }
}
