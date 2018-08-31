/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxmem.core;

import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxutils.unit.StorageUnit;

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
