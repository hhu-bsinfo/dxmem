package de.hhu.bsinfo.dxmem;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.RandomUtils;

public class DXMemoryCreateRemoveTest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXMemoryCreateRemoveTest.class.getSimpleName());

    @Test
    public void empty() {
        Configurator.setRootLevel(Level.TRACE);

        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);
        Assert.assertTrue(memory.analyze().analyze());
        memory.shutdown();
    }

    @Test
    public void createSimple() {
        Configurator.setRootLevel(Level.TRACE);

        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(DXMemoryTestConstants.CHUNK_SIZE_1);

        Assert.assertNotEquals(ChunkID.INVALID_ID, cid);

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(5, memory.analyze().getCIDTableTableEntries().size());
        Assert.assertEquals(1, memory.analyze().getCIDTableChunkEntries().size());
        Assert.assertEquals(0, memory.analyze().getCIDTableZombieEntries().size());

        memory.shutdown();
    }

    @Test
    public void createAndRemoveSimple() {
        Configurator.setRootLevel(Level.TRACE);

        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(DXMemoryTestConstants.CHUNK_SIZE_1);

        Assert.assertNotEquals(ChunkID.INVALID_ID, cid);

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(5, memory.analyze().getCIDTableTableEntries().size());
        Assert.assertEquals(1, memory.analyze().getCIDTableChunkEntries().size());
        Assert.assertEquals(0, memory.analyze().getCIDTableZombieEntries().size());

        memory.remove().remove(cid);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertEquals(5, memory.analyze().getCIDTableTableEntries().size());
        Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());
        Assert.assertEquals(0, memory.analyze().getCIDTableZombieEntries().size());

        memory.shutdown();
    }

    @Test
    public void createSize1() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, DXMemoryTestConstants.CHUNK_SIZE_1_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_1);
    }

    @Test
    public void createSize2() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, DXMemoryTestConstants.CHUNK_SIZE_2_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_2);
    }

    @Test
    public void createSize3() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, DXMemoryTestConstants.CHUNK_SIZE_3_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_3);
    }

    @Test
    public void createSize4() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, DXMemoryTestConstants.CHUNK_SIZE_4_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_4);
    }

    @Test
    public void createSize5() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_MEDIUM, DXMemoryTestConstants.CHUNK_SIZE_5_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_5);
    }

    @Test
    public void createSize6() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_LARGE, DXMemoryTestConstants.CHUNK_SIZE_6_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_6);
    }

    @Test
    public void createManySize1() {
        Configurator.setRootLevel(Level.DEBUG);
        createSize(DXMemoryTestConstants.HEAP_SIZE_LARGE, DXMemoryTestConstants.CHUNK_SIZE_1_MANY_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_1);
    }

    @Test
    public void createManySize2() {
        Configurator.setRootLevel(Level.DEBUG);
        createSize(DXMemoryTestConstants.HEAP_SIZE_LARGE, DXMemoryTestConstants.CHUNK_SIZE_2_MANY_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_2);
    }

    @Test
    public void createManySize3() {
        Configurator.setRootLevel(Level.DEBUG);
        createSize(DXMemoryTestConstants.HEAP_SIZE_LARGE, DXMemoryTestConstants.CHUNK_SIZE_3_MANY_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_3);
    }

    @Test
    public void createManySize4() {
        Configurator.setRootLevel(Level.DEBUG);
        createSize(DXMemoryTestConstants.HEAP_SIZE_LARGE, DXMemoryTestConstants.CHUNK_SIZE_4_MANY_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_4);
    }

    @Test
    public void createManySize5() {
        Configurator.setRootLevel(Level.DEBUG);
        createSize(DXMemoryTestConstants.HEAP_SIZE_LARGE, DXMemoryTestConstants.CHUNK_SIZE_5_MANY_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_5);
    }

    @Test
    public void createManySize6() {
        Configurator.setRootLevel(Level.DEBUG);
        createSize(DXMemoryTestConstants.HEAP_SIZE_LARGE, DXMemoryTestConstants.CHUNK_SIZE_6_MANY_COUNT,
                DXMemoryTestConstants.CHUNK_SIZE_6);
    }

    @Test
    public void createGraph() {
        Configurator.setRootLevel(Level.DEBUG);
        createSize(1024 * 1024 * 1024 * 4L, 100000000, 16);
    }

    @Test
    public void createFacebook() {
        Configurator.setRootLevel(Level.DEBUG);
        createSize(1024 * 1024 * 1024 * 8L, 100000000, 64);
    }

    @Test
    public void createYCSB() {
        Configurator.setRootLevel(Level.DEBUG);
        createSize(1024 * 1024 * 1024 * 12L, 10000000, 1000);
    }

    @Test
    public void mergeFreeBlocks() {
        Configurator.setRootLevel(Level.TRACE);
        createSizes(DXMemoryTestConstants.HEAP_SIZE_SMALL, 11, 1024);
    }

    @Test
    public void mergeFreeBlocks2() {
        Configurator.setRootLevel(Level.TRACE);
        createSizes(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1024, 11);
    }

    @Test
    public void mergeFreeBlocks3() {
        Configurator.setRootLevel(Level.TRACE);
        createSizes(DXMemoryTestConstants.HEAP_SIZE_SMALL, 8, 1024, 11);
    }

    @Test
    public void mergeFreeBlocks4() {
        Configurator.setRootLevel(Level.TRACE);
        createSizes(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1024, 5, 4561);
    }

    @Test
    public void mergeFreeBlocks5() {
        Configurator.setRootLevel(Level.TRACE);
        createSizes(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1024, 12, 5);
    }

    @Test
    public void mergeFreeBlocks6() {
        Configurator.setRootLevel(Level.TRACE);
        createSizes(DXMemoryTestConstants.HEAP_SIZE_SMALL, 2, 12, 5);
    }

    @Test
    public void mergeFreeBlocks7() {
        Configurator.setRootLevel(Level.TRACE);
        createSizes(DXMemoryTestConstants.HEAP_SIZE_SMALL, 2, 12, 5, 123, 832);
    }

    @Test
    public void createRandomSize() {
        Configurator.setRootLevel(Level.TRACE);
        // run this multiple times
        for (int i = 0; i < 1000; i++) {
            createRandomSizes(10, 0.5f);
        }
    }

    @Test
    public void createRandomSize2() {
        Configurator.setRootLevel(Level.DEBUG);
        // run this multiple times
        for (int i = 0; i < 1000; i++) {
            createRandomSizes(100, 0.5f);
        }
    }

    @Test
    public void createRandomSize3() {
        Configurator.setRootLevel(Level.DEBUG);
        // run this multiple times
        for (int i = 0; i < 1000; i++) {
            createRandomSizes(1000, 0.5f);
        }
    }

    @Test
    public void createRandomSize4() {
        Configurator.setRootLevel(Level.DEBUG);
        // run this multiple times
        for (int i = 0; i < 5; i++) {
            createRandomSizes(1000000, 0.1f);
        }
    }

    private void createSizes(final long p_heapSize, final int... p_sizes) {
        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, p_heapSize);

        long[] cids = new long[p_sizes.length];

        LOGGER.info("Creating %d chunks...", cids.length);

        for (int i = 0; i < p_sizes.length; i++) {
            cids[i] = memory.create().create(p_sizes[i]);
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());

        LOGGER.info("Deleting all created chunks...");

        for (int i = 0; i < cids.length; i++) {
            memory.remove().remove(cids[i]);
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());

        memory.shutdown();
    }

    private void createSize(final long p_heapSize, final int p_chunkCount, final int p_chunkSize) {
        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, p_heapSize);

        long[] cids = new long[p_chunkCount];

        LOGGER.info("Creating %d chunks with size %d...", p_chunkCount, p_chunkSize);

        for (int i = 0; i < cids.length; i++) {
            cids[i] = memory.create().create(p_chunkSize);
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());

        LOGGER.info("Deleting all created chunks...");

        for (int i = 0; i < cids.length; i++) {
            memory.remove().remove(cids[i]);
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());

        memory.shutdown();
    }

    private void createRandomSizes(final int p_count, final float p_additionalMemoryPercent) {
        int[] sizes = new int[p_count];
        long totalSize = 0;

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = RandomUtils.getRandomValue(1, 1024 * 4);
            totalSize += sizes[i];
        }

        LOGGER.info("Random chunk sizes with a total of %d bytes", totalSize);

        // add some more memory to ensure everything fits
        totalSize += (int) (totalSize * p_additionalMemoryPercent);

        // note: depending on the sizes created, this test might crash if you don't have sufficient memory available
        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID,
                totalSize < 1024 * 1024 ? DXMemoryTestConstants.HEAP_SIZE_SMALL : totalSize);

        long[] cids = new long[sizes.length];

        for (int i = 0; i < cids.length; i++) {
            cids[i] = memory.create().create(sizes[i]);
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());

        LOGGER.info("Deleting all created chunks...");

        for (int i = 0; i < cids.length; i++) {
            memory.remove().remove(cids[i]);
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());

        memory.shutdown();
    }
}
