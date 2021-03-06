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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.AllocationException;
import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.data.ChunkID;

public class OutOfMemoryTest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(OutOfMemoryTest.class.getSimpleName());

    @Test
    public void objectSize1() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 1);
    }

    @Test
    public void objectSize8() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 8);
    }

    @Test
    public void objectSize16() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 16);
    }

    @Test
    public void objectSize32() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 32);
    }

    @Test
    public void objectSize64() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 64);
    }

    @Test
    public void objectSize1024() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 1024);
    }

    @Test
    public void objectSize1MB() {
        Configurator.setRootLevel(Level.TRACE);
        createSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 1024 * 1024);
    }

    @Test
    public void multiObjectSize1() {
        Configurator.setRootLevel(Level.TRACE);
        createMultiSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 1);
    }

    @Test
    public void multiObjectSize8() {
        Configurator.setRootLevel(Level.TRACE);
        createMultiSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 8);
    }

    @Test
    public void multiObjectSize16() {
        Configurator.setRootLevel(Level.TRACE);
        createMultiSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 16);
    }

    @Test
    public void multiObjectSize32() {
        Configurator.setRootLevel(Level.TRACE);
        createMultiSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 32);
    }

    @Test
    public void multiObjectSize64() {
        Configurator.setRootLevel(Level.TRACE);
        createMultiSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 64);
    }

    @Test
    public void multiObjectSize1024() {
        Configurator.setRootLevel(Level.TRACE);
        createMultiSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 1024);
    }

    @Test
    public void multiObjectSize1MB() {
        Configurator.setRootLevel(Level.TRACE);
        createMultiSize(DXMemoryTestConstants.HEAP_SIZE_SMALL, 1000000, 1024 * 1024);
    }

    private static void createSize(final long p_heapSize, final int p_chunkCount, final int p_chunkSize) {
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, p_heapSize);

        long[] cids = new long[p_chunkCount];

        LOGGER.info("Creating %d chunks with size %d...", p_chunkCount, p_chunkSize);

        for (int i = 0; i < cids.length; i++) {
            try {
                cids[i] = memory.create().create(p_chunkSize);
            } catch (final AllocationException e) {
                LOGGER.info("Caught allocation exception", e);

                Assert.assertTrue(memory.analyze().analyze());
                memory.shutdown();
                return;
            }

            Assert.assertNotEquals(ChunkID.INVALID_ID, cids[i]);
        }

        Assert.fail("Heap was too large to to run out of memory");
    }

    private void createMultiSize(final long p_heapSize, final int p_size, final int p_count) {
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, p_heapSize);

        long[] cids = new long[p_count];

        LOGGER.info("Multi creating %d chunks with size %d...", p_count, p_size);

        memory.dump().dump("/tmp/asdf.bin");

        int successful = memory.create().create(cids, 0, p_count, p_size, false);

        if (successful != p_count) {
            LOGGER.info("Caught out of memory");

            Assert.assertTrue(memory.analyze().analyze());
            memory.shutdown();
            return;
        }

        Assert.fail("Heap was too large to to run out of memory");
    }
}
