package de.hhu.bsinfo.dxmem;

import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

public class DXMemTest {
    @Test
    public void insufficientMemory() {
       
        try {
            DXMem dxmem = new DXMem(DXMemoryTestConstants.NODE_ID, new StorageUnit(1, StorageUnit.TB).getBytes());
        } catch (MemoryRuntimeException ignored) {
            return;
        }

        Assert.fail("Insufficient memory exception not triggered");
    }
}
