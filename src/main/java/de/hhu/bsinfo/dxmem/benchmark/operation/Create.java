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
    private final int m_minSize;
    private final int m_maxSize;

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

        m_chunks = new ChunkBenchmark[1024];
    }

    @Override
    public ChunkState execute(final BenchmarkContext p_context, final boolean p_verifyData) {
        int size;

        if (m_minSize == m_maxSize) {
            size = m_minSize;
        } else {
            size = RandomUtils.getRandomValue(m_minSize, m_maxSize);
        }

        // throw allocation exceptions. the benchmark cannot continue once that happens
        executeTimeStart();
        long cid = p_context.create(size);
        executeTimeEnd();

        executeNewCid(cid);

        if (p_verifyData) {
            int tid = (int) Thread.currentThread().getId();

            if (m_chunks[tid] == null) {
                m_chunks[tid] = new ChunkBenchmark(m_maxSize);
            }

            m_chunks[tid].setID(cid);
            m_chunks[tid].setCurrentSize(size);
            m_chunks[tid].fillContents();

            p_context.put(m_chunks[tid]);

            if (!m_chunks[tid].isStateOk()) {
                return ChunkState.UNDEFINED;
            }
        }

        return cid != ChunkID.INVALID_ID ? ChunkState.OK : ChunkState.UNDEFINED;
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
