package de.hhu.bsinfo.dxmem;

import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;

public class DXMemoryPutGetExclusiveMultiThreadTest {
    private static final int CHUNK_SIZE = 1;

    @Test
    public void singleThread() {
        testThreaded(1, 1000);
    }

    @Test
    public void multiThread2() {
        testThreaded(2, 5000);
    }

    @Test
    public void multiThread4() {
        testThreaded(4, 250);
    }

    @Test
    public void multiThread8() {
        testThreaded(8, 200);
    }

    @Test
    public void multiThread16() {
        testThreaded(16, 100);
    }

    private void testThreaded(final int p_numThreads, final int p_iterations) {
        DXMemory memory = init();

        long cid = createChunk(memory);

        execute(memory, cid, p_numThreads, p_iterations);

        Assert.assertTrue(memory.analyze().analyze());
    }

    private void execute(final DXMemory p_memory, final long p_cid, final int p_numThreads, final int p_iterations) {
        Thread[] threads = new Thread[p_numThreads];

        for (int i = 0; i < p_numThreads; i++) {
            threads[i] = new Thread(() -> {
                ChunkByteArray chunk = new ChunkByteArray(p_cid, CHUNK_SIZE);

                for (int j = 0; j < p_iterations; j++) {
                    putGetExclusive(p_memory, chunk);
                }
            });
        }

        for (int i = 0; i < p_numThreads; i++) {
            threads[i].start();
        }

        for (int i = 0; i < p_numThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException ignored) {
            }
        }
    }

    private DXMemory init() {
        return new DXMemory(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);
    }

    private long createChunk(final DXMemory p_memory) {
        return p_memory.create().create(1);
    }

    private void putGetExclusive(final DXMemory p_memory, final ChunkByteArray p_chunk) {
        p_chunk.getData()[0] = (byte) Thread.currentThread().getId();

        p_memory.put().put(p_chunk, ChunkLockOperation.ACQUIRE_BEFORE_OP, -1);

        Assert.assertTrue(p_chunk.isStateOk());

        try {
            Thread.sleep(10);
        } catch (InterruptedException ignored) {

        }

        p_memory.get().get(p_chunk, ChunkLockOperation.RELEASE_AFTER_OP, -1);

        Assert.assertTrue(p_chunk.isStateOk());

        Assert.assertEquals((byte) Thread.currentThread().getId(), p_chunk.getData()[0]);
    }
}
