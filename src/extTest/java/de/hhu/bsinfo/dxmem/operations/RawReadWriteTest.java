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

public class RawReadWriteTest {
    @Test
    public void testByte() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        memory.rawWrite().writeByte(pinnedMemory.getAddress(), 0, (byte) 0xAA);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertEquals((byte) 0xAA, memory.rawRead().readByte(pinnedMemory.getAddress(), 0));

        memory.shutdown();
    }

    @Test
    public void testShort() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        memory.rawWrite().writeShort(pinnedMemory.getAddress(), 0, (short) 0xAABB);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertEquals((short) 0xAABB, memory.rawRead().readShort(pinnedMemory.getAddress(), 0));

        memory.shutdown();
    }

    @Test
    public void testChar() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        memory.rawWrite().writeChar(pinnedMemory.getAddress(), 0, 'X');

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertEquals('X', memory.rawRead().readChar(pinnedMemory.getAddress(), 0));

        memory.shutdown();
    }

    @Test
    public void testInt() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        memory.rawWrite().writeInt(pinnedMemory.getAddress(), 0, 0xAABBCCDD);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertEquals(0xAABBCCDD, memory.rawRead().readInt(pinnedMemory.getAddress(), 0));

        memory.shutdown();
    }

    @Test
    public void testLong() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        memory.rawWrite().writeLong(pinnedMemory.getAddress(), 0, 0xAABBCCDDEEFF1122L);

        Assert.assertTrue(memory.analyze().analyze());

        Assert.assertEquals(0xAABBCCDDEEFF1122L, memory.rawRead().readLong(pinnedMemory.getAddress(), 0));

        memory.shutdown();
    }

    @Test
    public void testByteArray() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(64);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        byte[] array = new byte[16];

        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) i;
        }

        memory.rawWrite().write(pinnedMemory.getAddress(), 0, array);

        Assert.assertTrue(memory.analyze().analyze());

        memory.rawRead().read(pinnedMemory.getAddress(), 0, array);

        for (int i = 0; i < array.length; i++) {
            Assert.assertEquals((byte) i, array[i]);
        }

        memory.shutdown();
    }

    @Test
    public void testShortArray() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(64);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        short[] array = new short[16];

        for (int i = 0; i < array.length; i++) {
            array[i] = (short) i;
        }

        memory.rawWrite().write(pinnedMemory.getAddress(), 0, array);

        Assert.assertTrue(memory.analyze().analyze());

        memory.rawRead().read(pinnedMemory.getAddress(), 0, array);

        for (int i = 0; i < array.length; i++) {
            Assert.assertEquals((short) i, array[i]);
        }

        memory.shutdown();
    }

    @Test
    public void testCharArray() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(64);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        char[] array = new char[16];

        for (int i = 0; i < array.length; i++) {
            array[i] = (char) i;
        }

        memory.rawWrite().write(pinnedMemory.getAddress(), 0, array);

        Assert.assertTrue(memory.analyze().analyze());

        memory.rawRead().read(pinnedMemory.getAddress(), 0, array);

        for (int i = 0; i < array.length; i++) {
            Assert.assertEquals((char) i, array[i]);
        }

        memory.shutdown();
    }

    @Test
    public void testIntArray() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(64);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        int[] array = new int[16];

        for (int i = 0; i < array.length; i++) {
            array[i] = i;
        }

        memory.rawWrite().write(pinnedMemory.getAddress(), 0, array);

        Assert.assertTrue(memory.analyze().analyze());

        memory.rawRead().read(pinnedMemory.getAddress(), 0, array);

        for (int i = 0; i < array.length; i++) {
            Assert.assertEquals(i, array[i]);
        }

        memory.shutdown();
    }

    @Test
    public void testLongArray() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(64);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        long[] array = new long[4];

        for (int i = 0; i < array.length; i++) {
            array[i] = i;
        }

        memory.rawWrite().write(pinnedMemory.getAddress(), 0, array);

        Assert.assertTrue(memory.analyze().analyze());

        memory.rawRead().read(pinnedMemory.getAddress(), 0, array);

        for (int i = 0; i < array.length; i++) {
            Assert.assertEquals(i, array[i]);
        }

        memory.shutdown();
    }
}
