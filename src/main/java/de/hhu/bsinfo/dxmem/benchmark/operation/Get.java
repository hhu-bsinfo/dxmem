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

import de.hhu.bsinfo.dxmem.benchmark.BenchmarkContext;
import de.hhu.bsinfo.dxmem.data.ChunkBenchmark;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;

/**
 * Get data from a chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Get extends AbstractOperation {
    private final int m_maxChunkSize;
    private final ChunkBenchmark[] m_chunks;

    /**
     * Constructor
     *
     * @param p_probability
     *         Operation probability (0.0 - 1.0)
     * @param p_batchCount
     *         Number of batches to execute for a single operation
     * @param p_verifyData
     *         True to enable data verification
     * @param p_maxChunkSize
     *         Max size of any chunk created
     */
    public Get(final float p_probability, final int p_batchCount, final boolean p_verifyData,
            final int p_maxChunkSize) {
        super("get", p_probability, p_batchCount, p_verifyData);

        m_maxChunkSize = p_maxChunkSize;

        m_chunks = new ChunkBenchmark[1024];
    }

    @Override
    public ChunkState execute(final BenchmarkContext p_context, final boolean p_verifyData) {
        long cid = executeGetRandomCid();

        // no chunks available, yet?
        if (cid == ChunkID.INVALID_ID) {
            return ChunkState.DOES_NOT_EXIST;
        }

        int tid = (int) Thread.currentThread().getId();

        if (m_chunks[tid] == null) {
            m_chunks[tid] = new ChunkBenchmark(m_maxChunkSize);
        }

        m_chunks[tid].setID(cid);

        executeTimeStart();
        p_context.get(m_chunks[tid]);
        executeTimeEnd();

        // verify data in chunk
        if (p_verifyData && m_chunks[tid].isStateOk()) {
            if (!m_chunks[tid].verifyContents()) {
                return ChunkState.DATA_LOST;
            }
        }

        return m_chunks[tid].getState();
    }
}
