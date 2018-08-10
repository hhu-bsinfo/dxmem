package de.hhu.bsinfo.dxmem.operations;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;

public class ReserveTest {
    @Test
    public void single() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.reserve().reserve();

        Assert.assertNotEquals(ChunkID.INVALID_ID, cid);

        memory.createReserved().createReserved(cid, 16);

        ChunkByteArray chunk = new ChunkByteArray(cid, 16);

        memory.get().get(chunk);
        Assert.assertTrue(chunk.isStateOk());

        Assert.assertTrue(memory.analyze().analyze());
        memory.shutdown();
    }

    @Test
    public void multi() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long[] cids = memory.reserve().reserve(10);

        for (int i = 0; i < cids.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
        }

        int[] sizes = new int[cids.length];

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = 16;
        }

        memory.createReservedMulti().createReserved(cids, null, sizes, 0, sizes.length);

        ChunkByteArray chunk = new ChunkByteArray(16);

        for (int i = 0; i < cids.length; i++) {
            chunk.setID(cids[i]);
            memory.get().get(chunk);
            Assert.assertTrue(chunk.isStateOk());
        }

        Assert.assertTrue(memory.analyze().analyze());
        memory.shutdown();
    }

    @Test
    public void multi2() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long[] cids = memory.reserve().reserve(10);
        ChunkByteArray[] chunks = new ChunkByteArray[10];

        for (int i = 0; i < cids.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
            chunks[i] = new ChunkByteArray(16);
            chunks[i].setID(cids[i]);
        }

        memory.createReservedMulti().createReserved(chunks, null);

        for (int i = 0; i < cids.length; i++) {
            chunks[i].setID(cids[i]);
            memory.get().get(chunks[i]);
            Assert.assertTrue(chunks[i].isStateOk());
        }

        Assert.assertTrue(memory.analyze().analyze());
        memory.shutdown();
    }

    @Test
    public void mult3() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long[] cids = new long[10];
        memory.reserve().reserve(cids, 0, cids.length);

        for (int i = 0; i < cids.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
        }

        int[] sizes = new int[cids.length];

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = 16;
        }

        memory.createReservedMulti().createReserved(cids, null, sizes, 0, sizes.length);

        ChunkByteArray chunk = new ChunkByteArray(16);

        for (int i = 0; i < cids.length; i++) {
            chunk.setID(cids[i]);
            memory.get().get(chunk);
            Assert.assertTrue(chunk.isStateOk());
        }

        Assert.assertTrue(memory.analyze().analyze());
        memory.shutdown();
    }
}
