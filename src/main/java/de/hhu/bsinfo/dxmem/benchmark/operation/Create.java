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
import de.hhu.bsinfo.dxutils.RandomUtils;

/**
 * Operation to create a chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Create extends AbstractOperation {
    private static final int MAX_THREADS = 1024;

    private final int m_minSize;
    private final int m_maxSize;

    private final long[][] m_cids;
    private final int[][] m_sizes;
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
     * @param p_minSize
     *         Min size of chunk to create
     * @param p_maxSize
     *         Max size of chunk to create
     */
    public Create(final float p_probability, final int p_batchCount, final boolean p_verifyData, final int p_minSize,
            final int p_maxSize) {
        super("create", p_probability, p_batchCount, p_verifyData);

        m_minSize = p_minSize;
        m_maxSize = p_maxSize;

        m_cids  = new long[MAX_THREADS][p_batchCount];
        m_sizes = new int[MAX_THREADS][p_batchCount];
        m_chunks = new ChunkBenchmark[MAX_THREADS][p_batchCount];
    }

    @Override
    public ChunkState execute(final BenchmarkContext p_context, final boolean p_verifyData) {
        int tid = (int) Thread.currentThread().getId();
        long[] cids = m_cids[tid];
        int[] sizes = m_sizes[tid];
        ChunkBenchmark[] chunks = m_chunks[tid];

        for (int i = 0; i < sizes.length; i++) {
            if (m_minSize == m_maxSize) {
                sizes[i] = m_minSize;
            } else {
                sizes[i] = RandomUtils.getRandomValue(m_minSize, m_maxSize);
            }
        }

        // throw allocation exceptions. the benchmark cannot continue once that happens
        executeTimeStart();
        p_context.create(cids, sizes);
        executeTimeEnd();

        executeNewCids(cids);

        // if verify data is on, we have to initialize that chunk with valid data.
        // otherwise, any following gets without previous puts will fail on data verification
        if (p_verifyData) {
            if (chunks[0] == null) {
                for (int i = 0; i < chunks.length; i++) {
                    chunks[i] = new ChunkBenchmark(m_maxSize);
                }
            }

            for (int i = 0; i < chunks.length; i++) {
                chunks[i].setID(cids[i]);
                chunks[i].setCurrentSize(sizes[i]);
                chunks[i].fillContents();
            }

            p_context.put(chunks);

            // return error of first failed chunk, only
            for (ChunkBenchmark chunk : chunks) {
                if (!chunk.isStateOk()) {
                    return chunk.getState();
                }
            }
        }

        // return error of first failed chunk, only
        for (long cid : cids) {
            if (cid == ChunkID.INVALID_ID) {
                return ChunkState.UNDEFINED;
            }
        }

        return ChunkState.OK;
    }

    @Override
    public String parameterToString() {
        StringBuilder builder = new StringBuilder();

        builder.append(super.parameterToString());
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(',');
        builder.append("minSize,");
        builder.append(m_minSize);
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(',');
        builder.append("maxSize,");
        builder.append(m_maxSize);

        return builder.toString();
    }
}
