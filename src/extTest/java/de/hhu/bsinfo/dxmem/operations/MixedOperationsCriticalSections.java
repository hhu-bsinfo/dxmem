package de.hhu.bsinfo.dxmem.operations;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkLockState;

public class MixedOperationsCriticalSections {
    @Test
    public void simpleCreateRemoveReadLock() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16, ChunkLockOperation.READ_LOCK_ACQ_POST_OP);

        ChunkLockState lockState = memory.lock().status(cid);

        Assert.assertEquals(cid, lockState.getCid());
        Assert.assertFalse(lockState.isWriteLocked());
        Assert.assertTrue(lockState.isReadLocked());
        Assert.assertEquals(1, lockState.getReadLockCount());

        int ret = memory.remove().remove(cid, ChunkLockOperation.READ_LOCK_SWAP_PRE_OP);

        Assert.assertEquals(16, ret);

        lockState = memory.lock().status(cid);

        Assert.assertNull(lockState);

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }

    @Test
    public void simpleCreateRemoveWriteLock() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16, ChunkLockOperation.WRITE_LOCK_ACQ_POST_OP);

        ChunkLockState lockState = memory.lock().status(cid);

        Assert.assertEquals(cid, lockState.getCid());
        Assert.assertTrue(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        int ret = memory.remove().remove(cid, ChunkLockOperation.NONE);

        Assert.assertEquals(16, ret);

        lockState = memory.lock().status(cid);

        Assert.assertNull(lockState);

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }

    @Test
    public void createGetPutRemoveWriteLocked() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);

        memory.create().create(ds, ChunkLockOperation.WRITE_LOCK_ACQ_POST_OP);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        ChunkLockState lockState = memory.lock().status(ds.getID());

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertTrue(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.put().put(ds, ChunkLockOperation.NONE, -1));

        Assert.assertTrue(ds.isStateOk());

        lockState = memory.lock().status(ds.getID());

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertTrue(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(memory.get().get(ds, ChunkLockOperation.NONE, -1));

        Assert.assertTrue(ds.isStateOk());

        lockState = memory.lock().status(ds.getID());

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertTrue(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.analyze().analyze());

        int ret = memory.remove().remove(ds, ChunkLockOperation.NONE);

        Assert.assertTrue(ds.isStateOk());
        Assert.assertEquals(ds.sizeofObject(), ret);

        lockState = memory.lock().status(ds.getID());

        Assert.assertNull(lockState);

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }

    @Test
    public void createPutWriteLock() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);

        memory.create().create(ds, ChunkLockOperation.WRITE_LOCK_ACQ_POST_OP);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        ChunkLockState lockState = memory.lock().status(ds.getID());

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertTrue(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.put().put(ds, ChunkLockOperation.WRITE_LOCK_REL_POST_OP, -1));

        Assert.assertTrue(ds.isStateOk());

        lockState = memory.lock().status(ds.getID());

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertFalse(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.analyze().analyze());

        int ret = memory.remove().remove(ds);

        Assert.assertTrue(ds.isStateOk());
        Assert.assertEquals(ds.sizeofObject(), ret);

        lockState = memory.lock().status(ds.getID());

        Assert.assertNull(lockState);

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }

    @Test
    public void createGetRemoveReadLock() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);

        memory.create().create(ds);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        ChunkLockState lockState = memory.lock().status(ds.getID());

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertFalse(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.get().get(ds, ChunkLockOperation.READ_LOCK_ACQ_PRE_OP, -1));

        Assert.assertTrue(ds.isStateOk());

        lockState = memory.lock().status(ds.getID());

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertFalse(lockState.isWriteLocked());
        Assert.assertTrue(lockState.isReadLocked());
        Assert.assertEquals(1, lockState.getReadLockCount());

        Assert.assertTrue(memory.analyze().analyze());

        int ret = memory.remove().remove(ds, ChunkLockOperation.READ_LOCK_SWAP_PRE_OP);

        Assert.assertTrue(ds.isStateOk());
        Assert.assertEquals(ds.sizeofObject(), ret);

        lockState = memory.lock().status(ds.getID());

        Assert.assertNull(lockState);

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }

    @Test
    public void createGetResizePutRemoveWriteLocked() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);

        memory.create().create(ds, ChunkLockOperation.WRITE_LOCK_ACQ_POST_OP);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        ChunkLockState lockState = memory.lock().status(ds.getID());

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertTrue(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.get().get(ds, ChunkLockOperation.NONE, -1));

        Assert.assertTrue(ds.isStateOk());

        lockState = memory.lock().status(ds.getID());

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertTrue(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.analyze().analyze());

        ds = new ChunkByteArray(ds.getID(), DXMemoryTestConstants.CHUNK_SIZE_2);

        memory.resize().resize(ds.getID(), ds.getSize(), ChunkLockOperation.NONE, -1);

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertTrue(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(memory.put().put(ds, ChunkLockOperation.NONE, -1));

        Assert.assertTrue(ds.isStateOk());

        lockState = memory.lock().status(ds.getID());

        Assert.assertEquals(ds.getID(), lockState.getCid());
        Assert.assertTrue(lockState.isWriteLocked());
        Assert.assertFalse(lockState.isReadLocked());
        Assert.assertEquals(0, lockState.getReadLockCount());

        Assert.assertTrue(memory.analyze().analyze());

        int ret = memory.remove().remove(ds, ChunkLockOperation.NONE);

        Assert.assertTrue(ds.isStateOk());
        Assert.assertEquals(ds.sizeofObject(), ret);

        lockState = memory.lock().status(ds.getID());

        Assert.assertNull(lockState);

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }
}
