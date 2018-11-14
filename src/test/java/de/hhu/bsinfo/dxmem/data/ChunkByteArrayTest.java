package de.hhu.bsinfo.dxmem.data;

import org.junit.Test;

public class ChunkByteArrayTest {
    @Test
    public void test16() throws ChunkTesterException {
        ChunkTester.testChunkInstance(() -> new ChunkByteArray(16), 10);
    }

    @Test
    public void test128() throws ChunkTesterException {
        ChunkTester.testChunkInstance(() -> new ChunkByteArray(128), 10);
    }

    @Test
    public void test1024() throws ChunkTesterException {
        ChunkTester.testChunkInstance(() -> new ChunkByteArray(1024), 10);
    }
}
