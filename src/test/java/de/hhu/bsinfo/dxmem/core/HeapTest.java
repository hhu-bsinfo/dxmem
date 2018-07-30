package de.hhu.bsinfo.dxmem.core;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

public class HeapTest {
    private static final long HEAP_SIZE_SMALL = 1024 * 1024;
    private static final long HEAP_SIZE_MEDIUM = 1024 * 1024 * 128;
    private static final long HEAP_SIZE_LARGE = 1024 * 1024 * 1024 * 4L;

    @Test
    public void test() {
        Configurator.setRootLevel(Level.TRACE);

        Heap heap = new Heap(HEAP_SIZE_SMALL);

        CIDTableChunkEntry entry = new CIDTableChunkEntry();

        Assert.assertTrue(heap.malloc(64, entry));
        Assert.assertTrue(entry.isAddressValid());

        heap.destroy();
    }
}
