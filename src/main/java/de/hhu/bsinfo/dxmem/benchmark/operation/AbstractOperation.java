package de.hhu.bsinfo.dxmem.benchmark.operation;

import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.stats.Time;
import de.hhu.bsinfo.dxutils.stats.TimePercentilePool;

public abstract class AbstractOperation {
    private final String m_name;
    private final float m_probability;
    private final int m_batchCount;
    private final boolean m_verifyData;

    private final TimePercentilePool m_time;
    private final AtomicLong m_opsRemaining;
    private final AtomicLong[] m_opsReturnCodes;

    private DXMem m_memory;
    private long m_totalOps;

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

    public float getProbability() {
        return m_probability;
    }

    public int getBatchCount() {
        return m_batchCount;
    }

    public boolean verifyData() {
        return m_verifyData;
    }

    public String getName() {
        return m_name;
    }

    public String getNameTag() {
        return '[' + m_name + ']';
    }

    public void init(final DXMem p_memory, final long p_totalOps) {
        m_memory = p_memory;
        m_totalOps = p_totalOps;

        m_opsRemaining.set(p_totalOps);

        for (AtomicLong atomic : m_opsReturnCodes) {
            atomic.set(0);
        }
    }

    public void finish() {
        m_time.sortValues();
    }

    public long getTotalOperations() {
        return m_totalOps;
    }

    public void incReturnCode(final ChunkState p_chunkState) {
        m_opsReturnCodes[p_chunkState.ordinal()].incrementAndGet();
    }

    public boolean consumeOperation() {
        return m_opsRemaining.get() > 0 && m_opsRemaining.decrementAndGet() >= 0;
    }

    public TimePercentilePool getTimePercentile() {
        return m_time;
    }

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
        builder.append(",TotalTime(ms),");
        builder.append(m_time.getTotalValue(Time.Prefix.MILLI));
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

    public ChunkState execute() {
        long timeStart = System.nanoTime();
        ChunkState state = execute(m_memory, m_verifyData);
        m_time.record(System.nanoTime() - timeStart);

        return state;
    }

    protected abstract ChunkState execute(final DXMem p_memory, final boolean p_verifyData);
}
