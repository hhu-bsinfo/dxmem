package de.hhu.bsinfo.dxmem.operations;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.core.Address;

public class PinningTest {
    @Test
    public void pin() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        Assert.assertTrue(pinnedMemory.isStateOk());
        Assert.assertNotEquals(Address.INVALID, pinnedMemory.getAddress());

        Assert.assertTrue(memory.analyze().analyze());

        memory.pinning().unpin(pinnedMemory.getAddress());

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }

    @Test
    public void pin2() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        Assert.assertTrue(pinnedMemory.isStateOk());
        Assert.assertNotEquals(Address.INVALID, pinnedMemory.getAddress());

        Assert.assertTrue(memory.analyze().analyze());

        memory.pinning().unpinCID(cid);

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }
}
