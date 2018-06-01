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

import de.hhu.bsinfo.dxmem.DXMemory;
import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * Allocate memory for an already reserved CID (reserved using the Reserve operation). This can also be
 * used by the recovery to write recovered (non local chunks).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public class CreateReserved {
    private static final Value SOP_CREATE_RESERVE = new Value(DXMemory.class, "CreateReserve");

    static {
        StatisticsManager.get().registerOperation(DXMemory.class, SOP_CREATE_RESERVE);
    }

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context with core components
     */
    public CreateReserved(final Context p_context) {
        m_context = p_context;
    }

    // DO NOT pass arbitrary IDs as the first parameter. use the reserve operation to generate
    // chunk IDs and reserve them
    // assigning arbitrary values will definitely break something

    /**
     * Allocate memory for a reserved CID. DO NOT pass arbitrary CIDs to this function. Always use the Reserve operation
     * to get CIDs which can be used with this operation. Using arbitrary CIDs will definitely break the subsystem.
     *
     * @param p_cid
     *         Reserved CID to allocate memory for
     * @param p_size
     *         Size of the chunk to allocate
     * @return Raw memory address of the chunk
     */
    public long createReserved(final long p_cid, final int p_size) {
        assert p_cid != ChunkID.INVALID_ID;
        assert p_size > 0;

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        m_context.getHeap().malloc(p_size, tableEntry);

        m_context.getCIDTable().insert(p_cid, tableEntry);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_CREATE_RESERVE.inc();

        return tableEntry.getAddress();
    }
}
