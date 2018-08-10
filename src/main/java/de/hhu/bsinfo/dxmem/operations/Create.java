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
import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.core.Context;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * Create a new chunk by generating a CID and allocating memory for it
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.06.2018
 */
public final class Create {
    private static final Value SOP_CREATE = new Value(DXMem.class, "Create");

    static {
        StatisticsManager.get().registerOperation(DXMem.class, SOP_CREATE);
    }

    private final Context m_context;

    /**
     * Constructor
     *
     * @param p_context
     *         CliContext with core components
     */
    public Create(final Context p_context) {
        m_context = p_context;
    }

    /**
     * Create a new chunk
     *
     * @param p_ds
     *         AbstractChunk to create/allocate memory for. On success, the resulting CID will be assigned to the
     *         AbstractChunk and the state is set to OK. If the operation failed, the state indicates the error.
     */
    public void create(final AbstractChunk p_ds) {
        p_ds.setID(create(p_ds.sizeofObject()));
        p_ds.setState(ChunkState.OK);
    }

    /**
     * Create a new chunk
     *
     * @param p_size
     *         Size of the chunk to create (payload size)
     * @return On success, CID assigned to the allocated memory for the chunk, ChunkID.INVALID_ID on failure
     */
    public long create(final int p_size) {
        assert p_size > 0;

        CIDTableChunkEntry tableEntry = m_context.getCIDTableEntryPool().get();

        m_context.getDefragmenter().acquireApplicationThreadLock();

        if (!m_context.getHeap().malloc(p_size, tableEntry)) {
            m_context.getDefragmenter().releaseApplicationThreadLock();

            throw new AllocationException(p_size);
        }

        long cid = ChunkID.getChunkID(m_context.getNodeId(), m_context.getLIDStore().get());
        m_context.getCIDTable().insert(cid, tableEntry);

        m_context.getDefragmenter().releaseApplicationThreadLock();

        SOP_CREATE.add(p_size);

        return cid;
    }
}
