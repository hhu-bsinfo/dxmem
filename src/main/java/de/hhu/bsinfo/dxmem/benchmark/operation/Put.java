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
import de.hhu.bsinfo.dxmem.data.ChunkBenchmark;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;

/**
 * Operation to put a chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Put extends AbstractOperation {
    private final ChunkBenchmark m_chunk;

    /**
     * Constructor
     *
     * @param p_probability
     *         Operation probability (0.0 - 1.0)
     * @param p_batchCount
     *         Number of batches to execute for a single operation
     * @param p_verifyData
     *         True to enable data verification
     * @param p_chunkSize
     *         Size of the chunk(s)
     */
    public Put(final float p_probability, final int p_batchCount, final boolean p_verifyData, final int p_chunkSize) {
        super("put", p_probability, p_batchCount, p_verifyData);

        m_chunk = new ChunkBenchmark(ChunkID.INVALID_ID, p_chunkSize);
    }

    @Override
    public ChunkState execute(final DXMem p_memory, final boolean p_verifyData) {
        long cid = executeGetRandomCid();

        // no chunks available, yet?
        if (cid == ChunkID.INVALID_ID) {
            return ChunkState.DOES_NOT_EXIST;
        }

        m_chunk.setID(cid);

        if (p_verifyData) {
            m_chunk.fillContents();
        }

        executeTimeStart();
        p_memory.put().put(m_chunk);
        executeTimeEnd();

        return m_chunk.getState();
    }
}
