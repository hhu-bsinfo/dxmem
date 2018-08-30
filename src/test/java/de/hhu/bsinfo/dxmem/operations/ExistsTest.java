package de.hhu.bsinfo.dxmem.operations;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;

public class ExistsTest {
    @Test
    public void exists() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);
        memory.create().create(ds);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        Assert.assertTrue(memory.exists().exists(ds.getID()));
        Assert.assertFalse(memory.exists().exists(1));

        memory.remove().remove(ds);
        Assert.assertTrue(ds.isStateOk());

        Assert.assertFalse(memory.exists().exists(ds.getID()));

        memory.shutdown();
    }
}
