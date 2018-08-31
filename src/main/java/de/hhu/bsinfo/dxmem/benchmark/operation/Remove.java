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

package de.hhu.bsinfo.dxmem.benchmark.operation;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;

/**
 * Remove a chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Remove extends AbstractOperation {
    private final ChunkByteArray m_chunk;

    /**
     * Constructor
     *
     * @param p_probability
     *         Operation probability (0.0 - 1.0)
     * @param p_batchCount
     *         Number of batches to execute for a single operation
     * @param p_verifyData
     *         True to enable data verification
     */
    public Remove(final float p_probability, final int p_batchCount, final boolean p_verifyData) {
        super("remove", p_probability, p_batchCount, p_verifyData);

        // dummy
        m_chunk = new ChunkByteArray(ChunkID.INVALID_ID, 1);
    }

    @Override
    public ChunkState execute(final DXMem p_memory, final boolean p_verifyData) {
        long cid = executeGetRandomCid();

        // no chunks available, yet?
        if (cid == ChunkID.INVALID_ID) {
            return ChunkState.DOES_NOT_EXIST;
        }

        m_chunk.setID(cid);

        executeTimeStart();
        p_memory.remove().remove(m_chunk);
        executeTimeEnd();

        executeRemoveCid(cid);

        return m_chunk.getState();
    }
}
