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

package de.hhu.bsinfo.dxmem.operations;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;

public class CidStatusTest {
    @Test
    public void highestUsedLocalID() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        Assert.assertEquals(ChunkID.INVALID_ID, memory.cidStatus().getHighestUsedLocalID());

        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);
        memory.create().create(ds);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertTrue(ds.isStateOk());
        Assert.assertTrue(ds.isIDValid());

        Assert.assertEquals(0, memory.cidStatus().getHighestUsedLocalID());

        memory.remove().remove(ds);
        Assert.assertTrue(ds.isStateOk());

        Assert.assertEquals(0, memory.cidStatus().getHighestUsedLocalID());

        memory.shutdown();
    }

    @Test
    public void chunkRangesSimple() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem((short) 0, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        Assert.assertEquals(new ChunkIDRanges(), memory.cidStatus().getCIDRangesOfChunks());

        create(memory, 1);

        Assert.assertEquals(new ChunkIDRanges(0, 0), memory.cidStatus().getCIDRangesOfChunks());

        memory.remove().remove(0);

        Assert.assertEquals(new ChunkIDRanges(), memory.cidStatus().getCIDRangesOfChunks());

        memory.shutdown();
    }

    @Test
    public void chunkRanges() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem((short) 0, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        Assert.assertEquals(new ChunkIDRanges(), memory.cidStatus().getCIDRangesOfChunks());

        create(memory, 100);

        ChunkIDRanges ranges = new ChunkIDRanges(0, 99);
        Assert.assertEquals(ranges, memory.cidStatus().getCIDRangesOfChunks());

        memory.remove().remove(0);
        ranges.remove(0);

        Assert.assertEquals(ranges, memory.cidStatus().getCIDRangesOfChunks());

        memory.remove().remove(5);
        ranges.remove(5);

        Assert.assertEquals(ranges, memory.cidStatus().getCIDRangesOfChunks());

        memory.shutdown();
    }

    private void create(final DXMem p_memory, final int p_count) {
        ChunkByteArray ds = new ChunkByteArray(DXMemoryTestConstants.CHUNK_SIZE_1);

        for (int i = 0; i < p_count; i++) {
            p_memory.create().create(ds);

            Assert.assertTrue(ds.isStateOk());
            Assert.assertTrue(ds.isIDValid());
        }

        Assert.assertTrue(p_memory.analyze().analyze());
    }
}
