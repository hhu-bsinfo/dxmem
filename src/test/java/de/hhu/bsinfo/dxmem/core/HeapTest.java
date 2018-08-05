package de.hhu.bsinfo.dxmem.core;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxutils.RandomUtils;

public class HeapTest {
    private static final long HEAP_SIZE_SMALL = 1024 * 1024;
    private static final long HEAP_SIZE_MEDIUM = 1024 * 1024 * 128;
    private static final long HEAP_SIZE_LARGE = 1024 * 1024 * 1024 * 4L;

    @Test
    public void mallocSimple() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(HEAP_SIZE_SMALL);

        CIDTableChunkEntry entry = new CIDTableChunkEntry();

        Assert.assertTrue(heap.malloc(64, entry));
        Assert.assertTrue(entry.isAddressValid());

        heap.destroy();
    }

    @Test
    public void malloc1() {
        Configurator.setRootLevel(Level.TRACE);
        mallocTest(HEAP_SIZE_SMALL, 1, 1, 100000, 1);
    }

    @Test
    public void malloc2() {
        Configurator.setRootLevel(Level.TRACE);
        mallocTest(HEAP_SIZE_MEDIUM, 1, 1, 10000000, 1);
    }

    @Test
    public void malloc3() {
        Configurator.setRootLevel(Level.TRACE);
        mallocTest(HEAP_SIZE_MEDIUM, 1024, 1024, 10000, 1);
    }

    @Test
    public void malloc4() {
        Configurator.setRootLevel(Level.TRACE);
        mallocTest(HEAP_SIZE_MEDIUM, 1, 128, 1000000, 1);
    }

    @Test
    public void malloc5() {
        Configurator.setRootLevel(Level.TRACE);
        mallocTest(HEAP_SIZE_MEDIUM, 1, 1, 10000000, 2);
    }

    @Test
    public void malloc6() {
        Configurator.setRootLevel(Level.TRACE);
        mallocTest(HEAP_SIZE_MEDIUM, 1, 1, 10000000, 4);
    }

    @Test
    public void malloc7() {
        Configurator.setRootLevel(Level.TRACE);
        mallocTest(HEAP_SIZE_LARGE, 1, 128, 10000000, 2);
    }

    @Test
    public void malloc8() {
        Configurator.setRootLevel(Level.TRACE);
        mallocTest(HEAP_SIZE_LARGE, 1, 128, 10000000, 4);
    }

    private void mallocTest(final long p_heapSize, final int p_chunkSizeMin, final int p_chunkSizeMax,
            final int p_allocCount, final int p_threads) {
        Heap heap = new Heap(p_heapSize);

        Thread[] threads = new Thread[p_threads];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                CIDTableChunkEntry entry = new CIDTableChunkEntry();

                for (int j = 0; j < p_allocCount / p_threads; j++) {
                    int size;

                    if (p_chunkSizeMin != p_chunkSizeMax) {
                        size = RandomUtils.getRandomValue(p_chunkSizeMin, p_chunkSizeMax);
                    } else {
                        size = p_chunkSizeMin;
                    }

                    Assert.assertTrue(heap.malloc(size, entry));
                    Assert.assertTrue(entry.isAddressValid());
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

        heap.destroy();
    }
}
