package de.hhu.bsinfo.dxmem;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;

public class DXMemoryGetPutTest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXMemoryGetPutTest.class.getSimpleName());

    @Test
    public void getSimple() {
        Configurator.setRootLevel(Level.TRACE);

        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);
        memory.create().create(ds);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        memory.get().get(ds);
        Assert.assertTrue(ds.isStateOk());

        memory.remove().remove(ds);
        Assert.assertTrue(ds.isStateOk());

        memory.shutdown();
    }

    @Test
    public void putGetSimple() {
        Configurator.setRootLevel(Level.TRACE);

        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);
        memory.create().create(ds);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        ds.getData()[0] = (byte) 0xAA;

        memory.put().put(ds);
        Assert.assertTrue(ds.isStateOk());

        // clear data
        ds.getData()[0] = 0;

        memory.get().get(ds);
        Assert.assertTrue(ds.isStateOk());
        Assert.assertEquals((byte) 0xAA, ds.getData()[0]);

        memory.remove().remove(ds);
        Assert.assertTrue(ds.isStateOk());

        memory.shutdown();
    }

    @Test
    public void putGetSize2() {
        Configurator.setRootLevel(Level.TRACE);
        putGetSize(DXMemoryTestConstants.CHUNK_SIZE_2);
    }

    @Test
    public void putGetSize3() {
        Configurator.setRootLevel(Level.TRACE);
        putGetSize(DXMemoryTestConstants.CHUNK_SIZE_3);
    }

    @Test
    public void putGetSize4() {
        Configurator.setRootLevel(Level.TRACE);
        putGetSize(DXMemoryTestConstants.CHUNK_SIZE_4);
    }

    @Test
    public void putGetSize5() {
        Configurator.setRootLevel(Level.TRACE);
        putGetSize(DXMemoryTestConstants.CHUNK_SIZE_5);
    }

    @Test
    public void putGetSize6() {
        Configurator.setRootLevel(Level.TRACE);
        putGetSize(DXMemoryTestConstants.CHUNK_SIZE_6);
    }

    private void putGetSize(final int p_size) {
        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID,
                p_size > DXMemoryTestConstants.HEAP_SIZE_SMALL * 0.8 ? (long) ((long) p_size + p_size * 0.2) :
                        DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(p_size);
        memory.create().create(ds);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        for (int i = 0; i < p_size; i++) {
            ds.getData()[i] = (byte) i;
        }

        memory.put().put(ds);
        Assert.assertTrue(ds.isStateOk());

        // clear data
        for (int i = 0; i < p_size; i++) {
            ds.getData()[i] = 0;
        }

        memory.get().get(ds);
        Assert.assertTrue(ds.isStateOk());

        for (int i = 0; i < p_size; i++) {
            Assert.assertEquals((byte) i, ds.getData()[i]);
        }

        memory.remove().remove(ds);
        Assert.assertTrue(ds.isStateOk());

        memory.shutdown();
    }
}
