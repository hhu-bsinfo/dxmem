package de.hhu.bsinfo.dxmem.benchmark.operation;

import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxmem.DXMemory;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxutils.stats.Time;
import de.hhu.bsinfo.dxutils.stats.TimePercentilePool;

public abstract class AbstractOperation {
    private final String m_name;
    private final float m_probability;
    private final int m_batchCount;

    private final TimePercentilePool m_time;
    private final AtomicLong m_opsRemaining;
    private final AtomicLong m_opsSuccessful;
    private final AtomicLong m_opsError;

    private DXMemory m_memory;
    private long m_totalOps;

    public AbstractOperation(final String p_name, final float p_probability, final int p_batchCount) {
        m_name = p_name;
        m_probability = p_probability;
        m_batchCount = p_batchCount;

        m_time = new TimePercentilePool(AbstractOperation.class, p_name);
        m_opsRemaining = new AtomicLong(0);
        m_opsSuccessful = new AtomicLong(0);
        m_opsError = new AtomicLong(0);
    }

    public float getProbability() {
        return m_probability;
    }

    public int getBatchCount() {
        return m_batchCount;
    }

    public String getName() {
        return m_name;
    }

    public String getNameTag() {
        return '[' + m_name + ']';
    }

    public void init(final DXMemory p_memory, final long p_totalOps) {
        m_memory = p_memory;
        m_totalOps = p_totalOps;

        m_opsRemaining.set(p_totalOps);
        m_opsSuccessful.set(0);
        m_opsError.set(0);
    }

    public void finish() {
        m_time.sortValues();
    }

    public long getTotalOperations() {
        return m_totalOps;
    }

    public void incSuccessful() {
        m_opsSuccessful.incrementAndGet();
    }

    public void incError() {
        m_opsError.incrementAndGet();
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
        builder.append(",TotalOperations,");
        builder.append(m_totalOps);
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",OperationsSuccessful,");
        builder.append(m_opsSuccessful.get());
        builder.append('\n');

        builder.append(getNameTag());
        builder.append(",OperationsError,");
        builder.append(m_opsError.get());
        builder.append('\n');

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
        ChunkState state = execute(m_memory);
        m_time.record(System.nanoTime() - timeStart);

        return state;
    }

    protected abstract ChunkState execute(final DXMemory p_memory);
}
