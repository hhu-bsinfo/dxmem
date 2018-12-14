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

public class ReserveTest {
    @Test
    public void single() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.reserve().reserve();

        Assert.assertNotEquals(ChunkID.INVALID_ID, cid);

        memory.createReserved().createReserved(cid, 16);

        ChunkByteArray chunk = new ChunkByteArray(cid, 16);

        memory.get().get(chunk);
        Assert.assertTrue(chunk.isStateOk());

        Assert.assertTrue(memory.analyze().analyze());
        memory.shutdown();
    }

    @Test
    public void multi() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long[] cids = memory.reserve().reserve(10);

        for (int i = 0; i < cids.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
        }

        int[] sizes = new int[cids.length];

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = 16;
        }

        memory.createReserved().createReserved(cids, null, sizes, 0, sizes.length);

        ChunkByteArray chunk = new ChunkByteArray(16);

        for (int i = 0; i < cids.length; i++) {
            chunk.setID(cids[i]);
            memory.get().get(chunk);
            Assert.assertTrue(chunk.isStateOk());
        }

        Assert.assertTrue(memory.analyze().analyze());
        memory.shutdown();
    }

    @Test
    public void multi2() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long[] cids = memory.reserve().reserve(10);
        ChunkByteArray[] chunks = new ChunkByteArray[10];

        for (int i = 0; i < cids.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
            chunks[i] = new ChunkByteArray(16);
            chunks[i].setID(cids[i]);
        }

        memory.createReserved().createReserved(chunks, null);

        for (int i = 0; i < cids.length; i++) {
            chunks[i].setID(cids[i]);
            memory.get().get(chunks[i]);
            Assert.assertTrue(chunks[i].isStateOk());
        }

        Assert.assertTrue(memory.analyze().analyze());
        memory.shutdown();
    }

    @Test
    public void mult3() {
        Configurator.setRootLevel(Level.TRACE);

        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long[] cids = new long[10];
        memory.reserve().reserve(cids, 0, cids.length);

        for (int i = 0; i < cids.length; i++) {
            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
        }

        int[] sizes = new int[cids.length];

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = 16;
        }

        memory.createReserved().createReserved(cids, null, sizes, 0, sizes.length);

        ChunkByteArray chunk = new ChunkByteArray(16);

        for (int i = 0; i < cids.length; i++) {
            chunk.setID(cids[i]);
            memory.get().get(chunk);
            Assert.assertTrue(chunk.isStateOk());
        }

        Assert.assertTrue(memory.analyze().analyze());
        memory.shutdown();
    }
}
