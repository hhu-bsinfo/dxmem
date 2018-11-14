package de.hhu.bsinfo.dxmem.data;

import org.junit.Test;

public class ChunkDummyTest {
    @Test
    public void test() throws ChunkTesterException {
        ChunkTester.testChunkInstance(ChunkDummy::new, 10);
    }
}
