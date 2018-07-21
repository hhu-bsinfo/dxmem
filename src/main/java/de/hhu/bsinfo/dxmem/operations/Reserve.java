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

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * Reserve CIDs without allocating memory. This might be useful for for loading tasks that
 * have to assign pointers/handles to entries of a data structure in order to connect them
 * to each other. Be careful when using this operation and make sure to actually use the
 * reserved CIDs with a CreateReserved operation. Throwing away any reserved CIDs creates
 * a sort of "memory leak" and the CIDs cannot be recovered by the system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public class Reserve {
    private static final Value SOP_RESERVE = new Value(DXMem.class, "Reserve");

    static {
        StatisticsManager.get().registerOperation(DXMem.class, SOP_RESERVE);
    }

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         Context with core components
     */
    public Reserve(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Reserve a CID but do not allocate memory for it. Allocation must be executed by the user explicitly using
     * the CreateReserved operation.
     *
     * @return New CID
     */
    public long reserve() {
        SOP_RESERVE.inc();

        m_context.getDefragmenter().releaseApplicationThreadLock();

        long cid = m_context.getLIDStore().get();

        m_context.getDefragmenter().releaseApplicationThreadLock();

        return cid;
    }
}
