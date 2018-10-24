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
    private static final int MAX_THREADS = 1024;

    private final int m_maxChunkSize;
    private final long[][] m_cids;
    private final ChunkBenchmark[][] m_chunks;

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

        m_cids = new long[MAX_THREADS][p_batchCount];
        m_chunks = new ChunkBenchmark[MAX_THREADS][p_batchCount];
    }

    @Override
    public ChunkState execute(final BenchmarkContext p_context, final boolean p_verifyData) {
        int tid = (int) Thread.currentThread().getId();
        long[] cids = m_cids[tid];
        ChunkBenchmark[] chunks = m_chunks[tid];

        executeGetRandomCids(cids, true);

        // no chunks available, yet?
        for (long cid : cids) {
            if (cid == ChunkID.INVALID_ID) {
                return ChunkState.DOES_NOT_EXIST;
            }
        }

        if (chunks[0] == null) {
            for (int i = 0; i < chunks.length; i++) {
                chunks[i] = new ChunkBenchmark(m_maxChunkSize);
            }
        }

        for (int i = 0; i < chunks.length; i++) {
            chunks[i].setID(cids[i]);
        }

        executeTimeStart();
        p_context.get(chunks);
        executeTimeEnd();

        // verify data in chunk
        if (p_verifyData) {
            for (ChunkBenchmark chunk : chunks) {
                if (chunk.isStateOk() && !chunk.verifyContents()) {
                    return ChunkState.DATA_LOST;
                }
            }
        }

        // return error of first failed chunk, only
        for (ChunkBenchmark chunk : chunks) {
            if (!chunk.isStateOk()) {
                return chunk.getState();
            }
        }

        return ChunkState.OK;
    }
}
