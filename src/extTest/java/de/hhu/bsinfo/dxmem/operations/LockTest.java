package de.hhu.bsinfo.dxmem.operations;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.data.ChunkLockState;
import de.hhu.bsinfo.dxmem.data.ChunkState;

public class LockTest {
    @Test
    public void readLock() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        ChunkState state = memory.lock().lock(cid, false, -1);

        Assert.assertEquals(ChunkState.OK, state);

        ChunkLockState lockState = memory.lock().status(cid);

        Assert.assertEquals(cid, lockState.getCid());
        Assert.assertFalse(lockState.isWriteLocked());
        Assert.assertTrue(lockState.isReadLocked());
        Assert.assertEquals(1, lockState.getReadLockCount());

        state = memory.lock().unlock(cid, false);

        Assert.assertEquals(ChunkState.OK, state);

        lockState = memory.lock().status(cid);

        Assert.assertEquals(cid, lockState.getCid());
        Assert.assertFalse(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }

    @Test
    public void writeLock() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        ChunkState state = memory.lock().lock(cid, true, -1);

        Assert.assertEquals(ChunkState.OK, state);

        ChunkLockState lockState = memory.lock().status(cid);

        Assert.assertEquals(cid, lockState.getCid());
        Assert.assertTrue(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        state = memory.lock().unlock(cid, true);

        Assert.assertEquals(ChunkState.OK, state);

        lockState = memory.lock().status(cid);

        Assert.assertEquals(cid, lockState.getCid());
        Assert.assertFalse(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }
}
