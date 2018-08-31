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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.stats.Time;
import de.hhu.bsinfo.dxutils.stats.TimePercentilePool;

/**
 * Base class for all operations offered by the benchmark framework
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public abstract class AbstractOperation {
    private final String m_name;
    private final float m_probability;
    private final int m_batchCount;
    private final boolean m_verifyData;

    private final TimePercentilePool m_time;
    private final AtomicLong m_opsRemaining;
    private final AtomicLong[] m_opsReturnCodes;

    private DXMem m_memory;
    private ChunkIDRanges m_cids;
    private ReentrantReadWriteLock m_cidsLock;
    private long m_totalOps;

    private long m_curStartTime;

    /**
     * Constructor
     *
     * @param p_name
     *         Name of the operation
     * @param p_probability
     *         Probability (0.0 - 1.0)
     * @param p_batchCount
     *         Batch count
     * @param p_verifyData
     *         True to verify data with the operation, false to disable data verification
     */
    public AbstractOperation(final String p_name, final float p_probability, final int p_batchCount,
            final boolean p_verifyData) {
        m_name = p_name;
        m_probability = p_probability;
        m_batchCount = p_batchCount;
        m_verifyData = p_verifyData;

        m_time = new TimePercentilePool(AbstractOperation.class, p_name);
        m_opsRemaining = new AtomicLong(0);
        m_opsReturnCodes = new AtomicLong[ChunkState.values().length];

        for (int i = 0; i < m_opsReturnCodes.length; i++) {
            m_opsReturnCodes[i] = new AtomicLong(0);
        }
    }

    /**
     * Get the probability for the operation
     *
     * @return Probability (0.0 - 1.0)
     */
    public float getProbability() {
        return m_probability;
    }

    /**
     * Get the batch count for the operation
     *
     * @return Batch count
     */
    public int getBatchCount() {
        return m_batchCount;
    }

    /**
     * Check if data must be verified with the operation
     *
     * @return True to verify data
     */
    public boolean verifyData() {
        return m_verifyData;
    }

    /**
     * Get the name of the operation
     *
     * @return Name
     */
    public String getName() {
        return m_name;
    }

    /**
     * Get the name tag (e.g. [test]) of the operation
     *
     * @return Name tag
     */
    public String getNameTag() {
        return '[' + m_name + ']';
    }

    /**
     * Initialize the operation
     *
     * @param p_memory
     *         Instance of DXMem to operate on
     * @param p_cids
     *         Chunk ranges for operation to use
     * @param p_cidsLock
     *         Lock for concurrent access to chunk ranges
     * @param p_totalOps
     *         Total operations to execute
     */
    public void init(final DXMem p_memory, final ChunkIDRanges p_cids, final ReentrantReadWriteLock p_cidsLock,
            final long p_totalOps) {
        m_memory = p_memory;
        m_cids = p_cids;
        m_cidsLock = p_cidsLock;
        m_totalOps = p_totalOps;

        m_opsRemaining.set(p_totalOps);

        for (AtomicLong atomic : m_opsReturnCodes) {
            atomic.set(0);
        }
    }

    /**
     * Call this once the benchmark is finished
     */
    public void finish() {
        m_time.sortValues();
    }

    /**
     * Get the total operations to excecute
     *
     * @return Total operations to execute
     */
    public long getTotalOperations() {
        return m_totalOps;
    }

    /**
     * Get the number of return codes for a specific state
     *
     * @param p_state
     *         State
     * @return Number of return codes of specified state
     */
    public long getNumReturnCodes(final ChunkState p_state) {
        return m_opsReturnCodes[p_state.ordinal()].get();
    }

    /**
     * Increment the return code counter for a specific state
     *
     * @param p_chunkState
     *         State to increment the counter of
     */
    public void incReturnCode(final ChunkState p_chunkState) {
        m_opsReturnCodes[p_chunkState.ordinal()].incrementAndGet();
    }

    /**
     * Consume one operation
     *
     * @return True if successful, false if no operations are left to consume
     */
    public boolean consumeOperation() {
        long opsRemain;

        do {
            opsRemain = m_opsRemaining.get();

            if (opsRemain == 0) {
                return false;
            }
        } while (!m_opsRemaining.compareAndSet(opsRemain, opsRemain - 1));

        return true;
    }

    /**
     * Get the TimePercentilePool used for measuring operation execution
     *
     * @return TimePercentilePool
     */
    public TimePercentilePool getTimePercentile() {
        return m_time;
    }

    /**
     * Generate an output string of the operation and it's stats
     *
     * @return Readable output string of operation state
     */
    public String parameterToString() {
        StringBuilder builder = new StringBuilder();

        builder.append(getNameTag());
        builder.append(",Probability,");
        builder.append(m_probability);
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",BatchCount,");
        builder.append(m_batchCount);
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",VerifyData,");
        builder.append(m_verifyData);
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",TotalOperations,");
        builder.append(m_totalOps);
        builder.append('\n');

        for (int i = 0; i < m_opsReturnCodes.length; i++) {
            builder.append(getNameTag());
            builder.append(",OperationReturnCode(");
            builder.append(ChunkState.values()[i]);
            builder.append("),");
            builder.append(m_opsReturnCodes[i].get());
            builder.append('\n');
        }

        builder.append(getNameTag());
        builder.append(",TotalTimeAllThreadsAggregated(ms),");
        builder.append(m_time.getTotalValue(Time.Prefix.MILLI));
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",ThroughputAvgPerThread(mops/sec),");
        builder.append(m_totalOps / m_time.getTotalValue(Time.Prefix.SEC) / 1000000);
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",LatencyAvg(us),");
        builder.append(m_time.getAvg(Time.Prefix.MICRO));
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",LatMin(us),");
        builder.append(m_time.getMin(Time.Prefix.MICRO));
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",LatMax(us),");
        builder.append(m_time.getMax(Time.Prefix.MICRO));
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",Lat95(us),");
        builder.append(m_time.getPercentileScore(0.95f, Time.Prefix.MICRO));
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",Lat99(us),");
        builder.append(m_time.getPercentileScore(0.99f, Time.Prefix.MICRO));
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",Lat999(us),");
        builder.append(m_time.getPercentileScore(0.999f, Time.Prefix.MICRO));

        return builder.toString();
    }

    /**
     * Execute the operation
     *
     * @return ChunkState of the operation executed
     */
    public ChunkState execute() {
        return execute(m_memory, m_verifyData);
    }

    /**
     * Implement this to execute your operation
     *
     * @param p_memory
     *         Instance of DXMem to execute the operation on
     * @param p_verifyData
     *         True to verify data after execution (if possible), false otherwise
     * @return ChunkState result of execution
     */
    protected abstract ChunkState execute(final DXMem p_memory, final boolean p_verifyData);

    /**
     * Call this to start measuring time once you execute your operation
     */
    protected void executeTimeStart() {
        m_curStartTime = System.nanoTime();
    }

    /**
     * Call this to stop measuring time after you executed your operation
     */
    protected void executeTimeEnd() {
        m_time.record(System.nanoTime() - m_curStartTime);
    }

    /**
     * Call this if you generated a new cid when executing your operation
     *
     * @param p_cid
     *         New cid generated
     */
    protected void executeNewCid(final long p_cid) {
        m_cidsLock.writeLock().lock();
        m_cids.add(p_cid);
        m_cidsLock.writeLock().unlock();
    }

    /**
     * Get a random cid for executing your operation
     *
     * @return Random cid
     */
    protected long executeGetRandomCid() {
        long tmp;

        m_cidsLock.readLock().lock();
        tmp = m_cids.getRandomCidWithinRanges();
        m_cidsLock.readLock().unlock();

        return tmp;
    }

    /**
     * Call this if you removed a chunk
     *
     * @param p_cid
     *         Cid of chunk removed
     */
    protected void executeRemoveCid(final long p_cid) {
        m_cidsLock.writeLock().lock();
        m_cids.remove(p_cid);
        m_cidsLock.writeLock().unlock();
    }
}
