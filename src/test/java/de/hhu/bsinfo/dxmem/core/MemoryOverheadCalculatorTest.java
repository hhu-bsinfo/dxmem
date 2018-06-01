package de.hhu.bsinfo.dxmem.core;

import de.hhu.bsinfo.dxutils.unit.StorageUnit;
import org.junit.Assert;
import org.junit.Test;

public class MemoryOverheadCalculatorTest {
    @Test
    public void simple() {
        MemoryOverheadCalculator calc = new MemoryOverheadCalculator(64, 1000);

        Assert.assertEquals(64, calc.getChunkPayloadSize());
        Assert.assertEquals(1000, calc.getTotalChunkCount());
        Assert.assertEquals(64 * 1000, calc.getTotalPayloadMem().getBytes());
        Assert.assertEquals(65000, calc.getChunkSizeMemory().getBytes());
        Assert.assertEquals(new StorageUnit(512, "kb"), calc.getNIDTableSize());
        Assert.assertEquals(new StorageUnit(32, "kb"), calc.getLIDTableSizes()[0]);
        Assert.assertEquals(new StorageUnit(32, "kb"), calc.getLIDTableSizes()[1]);
        Assert.assertEquals(new StorageUnit(32, "kb"), calc.getLIDTableSizes()[2]);
        Assert.assertEquals(new StorageUnit(32, "kb"), calc.getLIDTableSizes()[3]);
        Assert.assertEquals(720360, calc.getTotalMem().getBytes());
        Assert.assertEquals(656360, calc.getOverheadMem().getBytes());
        Assert.assertEquals(91.0f, calc.getOverhead(), 1.0f);
    }

    @Test
    public void simple2() {
        MemoryOverheadCalculator calc = new MemoryOverheadCalculator(2048, 1000);

        Assert.assertEquals(2048, calc.getChunkPayloadSize());
        Assert.assertEquals(1000, calc.getTotalChunkCount());
        Assert.assertEquals(2048 * 1000, calc.getTotalPayloadMem().getBytes());
        Assert.assertEquals(2048 * 1000 + 2 * 1000, calc.getChunkSizeMemory().getBytes());
        Assert.assertEquals(new StorageUnit(512, "kb"), calc.getNIDTableSize());
        Assert.assertEquals(new StorageUnit(32, "kb"), calc.getLIDTableSizes()[0]);
        Assert.assertEquals(new StorageUnit(32, "kb"), calc.getLIDTableSizes()[1]);
        Assert.assertEquals(new StorageUnit(32, "kb"), calc.getLIDTableSizes()[2]);
        Assert.assertEquals(new StorageUnit(32, "kb"), calc.getLIDTableSizes()[3]);
        Assert.assertEquals(2705360, calc.getTotalMem().getBytes());
        Assert.assertEquals(657360, calc.getOverheadMem().getBytes());
        Assert.assertEquals(24.0f, calc.getOverhead(), 1.0f);
    }
}
