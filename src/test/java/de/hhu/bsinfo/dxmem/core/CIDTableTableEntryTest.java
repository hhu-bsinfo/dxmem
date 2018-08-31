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

public class CIDTableTableEntryTest {
    private long m_pointer = 0x198374621L;
    private long m_address = 0xAA723BBCCL;
    private long m_value = m_address << CIDTableTableEntry.OFFSET_ADDRESS;

    @Test
    public void test() {
        CIDTableTableEntry entry1 = new CIDTableTableEntry();
        CIDTableTableEntry entry2 = new CIDTableTableEntry();

        Assert.assertEquals(false, entry1.isAddressValid());

        entry1.setAddress(m_address);
        entry1.setPointer(m_pointer);

        Assert.assertEquals(m_address, entry1.getAddress());

        Assert.assertEquals(true, entry1.isAddressValid());
        Assert.assertEquals(m_pointer, entry1.getPointer());

        entry2.set(m_pointer, m_value);

        Assert.assertEquals(entry2.getValue(), entry1.getValue());
        Assert.assertEquals(entry2.getAddress(), entry1.getAddress());
        Assert.assertEquals(entry2.getPointer(), entry1.getPointer());
    }
}
