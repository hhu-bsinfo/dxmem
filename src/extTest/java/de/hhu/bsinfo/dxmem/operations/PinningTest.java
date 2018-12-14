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
import de.hhu.bsinfo.dxmem.core.Address;

public class PinningTest {
    @Test
    public void pin() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        Assert.assertTrue(pinnedMemory.isStateOk());
        Assert.assertNotEquals(Address.INVALID, pinnedMemory.getAddress());

        Assert.assertTrue(memory.analyze().analyze());

        memory.pinning().unpin(pinnedMemory.getAddress());

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }

    @Test
    public void pin2() {
        Configurator.setRootLevel(Level.TRACE);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_SMALL);

        long cid = memory.create().create(16);

        Pinning.PinnedMemory pinnedMemory = memory.pinning().pin(cid);

        Assert.assertTrue(pinnedMemory.isStateOk());
        Assert.assertNotEquals(Address.INVALID, pinnedMemory.getAddress());

        Assert.assertTrue(memory.analyze().analyze());

        memory.pinning().unpinCID(cid);

        Assert.assertTrue(memory.analyze().analyze());

        memory.shutdown();
    }
}
