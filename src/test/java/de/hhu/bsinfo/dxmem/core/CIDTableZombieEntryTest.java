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

import de.hhu.bsinfo.dxmem.data.ChunkID;

public class CIDTableZombieEntryTest {
    private long m_pointer = 0x198374621L;
    private long m_cid = 0x12341234124L;

    @Test
    public void test() {
        CIDTableZombieEntry entry1 = new CIDTableZombieEntry();
        Assert.assertEquals(Address.INVALID, entry1.getPointer());
        Assert.assertEquals(ChunkID.INVALID_ID, entry1.getCID());

        entry1.set(m_pointer, m_cid);
        Assert.assertEquals(m_pointer, entry1.getPointer());
        Assert.assertEquals(m_cid, entry1.getCID());

        entry1.clear();
        Assert.assertEquals(Address.INVALID, entry1.getPointer());
        Assert.assertEquals(ChunkID.INVALID_ID, entry1.getCID());

        entry1 = new CIDTableZombieEntry(m_pointer, m_cid);
        Assert.assertEquals(m_pointer, entry1.getPointer());
        Assert.assertEquals(m_cid, entry1.getCID());
    }
}
