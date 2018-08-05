package de.hhu.bsinfo.dxmem;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Heap;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.RandomUtils;

public class DXMemCreateRemoveTest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXMemCreateRemoveTest.class.getSimpleName());

    @Test
    public void empty() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);
        Assert.assertTrue(memory.analyze().analyze());
        memory.shutdown();
    }

    @Test
    public void createSimple() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

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

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

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
    public void createTestChunk() {
        createTestChunk(DXMemoryTestConstants.HEAP_SIZE_MEDIUM, 1);
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
    public void createTestChunkMany1() {
        createTestChunk(DXMemoryTestConstants.HEAP_SIZE_MEDIUM, 10);
    }

    @Test
    public void createTestChunkMany2() {
        createTestChunk(DXMemoryTestConstants.HEAP_SIZE_MEDIUM, 100);
    }

    @Test
    public void createTestChunkMany3() {
        createTestChunk(DXMemoryTestConstants.HEAP_SIZE_LARGE, 1000);
    }

    @Test
    public void createTestChunkManyRandom() {
        for (int i = 0; i < 10; i++) {
            createTestChunk(DXMemoryTestConstants.HEAP_SIZE_LARGE, RandomUtils.getRandomValue(0, 1000));
        }
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

    @Test
    public void createMultiEqualSized1() {
        Configurator.setRootLevel(Level.DEBUG);
        createMultiSameSize(1, 100, 0.1f, false);
    }

    @Test
    public void createMultiEqualSized2() {
        Configurator.setRootLevel(Level.DEBUG);
        createMultiSameSize(1, 100, 0.1f, true);
    }

    @Test
    public void createMultiEqualSized3() {
        Configurator.setRootLevel(Level.DEBUG);
        createMultiSameSize(64, 100, 0.1f, false);
    }

    @Test
    public void createMultiEqualSized4() {
        Configurator.setRootLevel(Level.DEBUG);
        createMultiSameSize(64, 100, 0.1f, true);
    }

    @Test
    public void createMultiRandomSize1() {
        Configurator.setRootLevel(Level.DEBUG);

        for (int i = 0; i < 10; i++) {
            createMultiRandomSizes(100, 0.1f, false);
        }
    }

    @Test
    public void createMultiRandomSize2() {
        Configurator.setRootLevel(Level.DEBUG);

        for (int i = 0; i < 10; i++) {
            createMultiRandomSizes(100, 0.1f, true);
        }
    }

    @Test
    public void createMultiRandomSize3() {
        Configurator.setRootLevel(Level.DEBUG);

        for (int i = 0; i < 10; i++) {
            createMultiRandomSizes(10000, 0.1f, false);
        }
    }

    @Test
    public void createMultiRandomSize4() {
        Configurator.setRootLevel(Level.DEBUG);

        for (int i = 0; i < 10; i++) {
            createMultiRandomSizes(10000, 0.1f, true);
        }
    }

    @Test
    public void createAndRemoveRepetetive1() {
        Configurator.setRootLevel(Level.DEBUG);
        createAndRemoveRepetitive(DXMemoryTestConstants.HEAP_SIZE_MEDIUM, 100, 16);
    }

    @Test
    public void createAndRemoveRepetetive2() {
        Configurator.setRootLevel(Level.DEBUG);
        createAndRemoveRepetitive(DXMemoryTestConstants.HEAP_SIZE_MEDIUM, 1000, 16);
    }

    @Test
    public void createAndRemoveRepetetive3() {
        Configurator.setRootLevel(Level.DEBUG);
        createAndRemoveRepetitive(DXMemoryTestConstants.HEAP_SIZE_LARGE, 100000 + 1, 1);
    }

    @Test
    public void createAndRemoveRepetetive4() {
        Configurator.setRootLevel(Level.DEBUG);
        createAndRemoveRepetitive(DXMemoryTestConstants.HEAP_SIZE_LARGE, 1000000, 1);
    }

    @Test
    public void createMultiThreaded1() {
        Configurator.setRootLevel(Level.DEBUG);
        createMultiThreaded(DXMemoryTestConstants.HEAP_SIZE_MEDIUM, 1, 1, 10000000, 2);
    }

    @Test
    public void createMultiThreaded2() {
        Configurator.setRootLevel(Level.DEBUG);
        createMultiThreaded(DXMemoryTestConstants.HEAP_SIZE_MEDIUM, 1, 1, 10000000, 4);
    }

    private void createMultiThreaded(final long p_heapSize, final int p_chunkSizeMin, final int p_chunkSizeMax,
            final int p_allocCount, final int p_threads) {
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, p_heapSize);

        Thread[] threads = new Thread[p_threads];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < p_allocCount / p_threads; j++) {
                    int size;

                    if (p_chunkSizeMin != p_chunkSizeMax) {
                        size = RandomUtils.getRandomValue(p_chunkSizeMin, p_chunkSizeMax);
                    } else {
                        size = p_chunkSizeMin;
                    }

                    long cid = memory.create().create(size);
                    Assert.assertNotEquals(ChunkID.INVALID_ID, cid);
                }
            });
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException ignored) {
            }
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }

    private static void createSizes(final long p_heapSize, final int... p_sizes) {
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, p_heapSize);

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
            Assert.assertEquals(p_sizes[i], memory.remove().remove(cids[i]));
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());

        memory.shutdown();
    }

    private static void createSize(final long p_heapSize, final int p_chunkCount, final int p_chunkSize) {
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, p_heapSize);

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
            Assert.assertEquals(p_chunkSize, memory.remove().remove(cids[i]));
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());

        memory.shutdown();
    }

    private static void createRandomSizes(final int p_count, final float p_additionalMemoryPercent) {
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
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID,
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
            Assert.assertEquals(sizes[i], memory.remove().remove(cids[i]));
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());

        memory.shutdown();
    }

    private void createTestChunk(final long p_heapSize, final int p_count) {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, p_heapSize);

        TestChunk[] chunks = new TestChunk[p_count];

        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = new TestChunk(true);
        }

        LOGGER.info("Creating %d chunks with size %d...", p_count, chunks[0].sizeofObject());

        for (TestChunk chunk : chunks) {
            memory.create().create(chunk);
            Assert.assertEquals(ChunkState.OK, chunk.getState());
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());

        LOGGER.info("Deleting all created chunks...");

        for (TestChunk chunk : chunks) {
            memory.remove().remove(chunk);
            Assert.assertEquals(ChunkState.OK, chunk.getState());
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());

        memory.shutdown();
    }

    private void createMultiRandomSizes(final int p_count, final float p_additionalMemoryPercent,
            final boolean p_consecutiveIds) {
        int[] sizes = new int[p_count];
        long totalSize = 0;

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = RandomUtils.getRandomValue(1, 1024 * 4);
            totalSize += sizes[i];
        }

        LOGGER.info("Random multi chunk sizes with a total of %d bytes", totalSize);

        // add some more memory to ensure everything fits
        totalSize += (int) (totalSize * p_additionalMemoryPercent);

        // note: depending on the sizes created, this test might crash if you don't have sufficient memory available
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID,
                totalSize < 1024 * 1024 ? DXMemoryTestConstants.HEAP_SIZE_SMALL : totalSize);

        long[] cids = new long[sizes.length];

        Assert.assertEquals(cids.length, memory.createMulti().create(cids, 0, p_consecutiveIds, sizes));

        for (int i = 0; i < cids.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());

        LOGGER.info("Deleting all created chunks...");

        for (int i = 0; i < cids.length; i++) {
            Assert.assertEquals(sizes[i], memory.remove().remove(cids[i]));
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());

        memory.shutdown();
    }

    private void createMultiSameSize(final int p_size, final int p_count, final float p_additionalMemoryPercent,
            final boolean p_consecutiveIds) {

        LOGGER.info("Equally sized multi chunk sizes with a total of %d bytes", p_size * p_count);

        // add some more memory to ensure everything fits
        long totalSize = (long) (p_size * p_count * p_additionalMemoryPercent);

        // note: depending on the sizes created, this test might crash if you don't have sufficient memory available
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID,
                totalSize < 1024 * 1024 ? DXMemoryTestConstants.HEAP_SIZE_SMALL : totalSize);

        long[] cids = new long[p_count];

        Assert.assertEquals(cids.length, memory.createMulti().create(cids, 0, p_count, p_size, p_consecutiveIds));

        for (int i = 0; i < cids.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());

        LOGGER.info("Deleting all created chunks...");

        for (int i = 0; i < cids.length; i++) {
            Assert.assertEquals(p_size, memory.remove().remove(cids[i]));
        }

        LOGGER.info("Done");

        Assert.assertTrue(memory.analyze().analyze());
        Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());

        memory.shutdown();
    }

    // to create and test zombie entries
    private static void createAndRemoveRepetitive(final long p_heapSize, final int p_chunkCount,
            final int p_chunkSize) {
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, p_heapSize);

        long[] cids = new long[p_chunkCount];

        for (int j = 0; j < 5; j++) {
            LOGGER.info("Creating %d chunks with size %d...", p_chunkCount, p_chunkSize);

            for (int i = 0; i < cids.length; i++) {
                cids[i] = memory.create().create(p_chunkSize);
                Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
            }

            LOGGER.info("Done");

            Assert.assertTrue(memory.analyze().analyze());

            LOGGER.info("Deleting all created chunks...");

            for (int i = 0; i < cids.length; i++) {
                Assert.assertEquals(p_chunkSize, memory.remove().remove(cids[i]));
            }

            LOGGER.info("Done");

            Assert.assertTrue(memory.analyze().analyze());
            Assert.assertEquals(0, memory.analyze().getCIDTableChunkEntries().size());
        }

        memory.shutdown();
    }
}
