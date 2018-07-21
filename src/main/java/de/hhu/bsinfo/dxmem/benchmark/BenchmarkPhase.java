package de.hhu.bsinfo.dxmem.benchmark;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.benchmark.operation.AbstractOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxmonitor.progress.CpuProgress;
import de.hhu.bsinfo.dxmonitor.state.MemState;
import de.hhu.bsinfo.dxmonitor.state.StateUpdateException;
import de.hhu.bsinfo.dxutils.stats.Time;
import de.hhu.bsinfo.dxutils.stats.TimePercentile;

public class BenchmarkPhase {
    private final String m_name;
    private final int m_numThreads;
    private final long m_totalNumOperations;
    private final long m_delayNsBetweenOps;
    private final AbstractOperation[] m_operations;

    private Thread[] m_threads;
    private AtomicInteger m_threadsRunning;
    private long m_totalTimeNs;

    public BenchmarkPhase(final String p_name, final DXMem p_memory, final int p_numThreads,
            final long p_totalNumOperations, final long p_delayNsBetweenOps, final AbstractOperation... p_operations) {
        m_name = p_name;
        m_numThreads = p_numThreads;
        m_totalNumOperations = p_totalNumOperations;
        m_delayNsBetweenOps = p_delayNsBetweenOps;
        m_operations = p_operations;

        // check, probabilities have to sum up to 1.0
        float totalProb = 0.0f;

        for (AbstractOperation op : m_operations) {
            totalProb += op.getProbability();
        }

        if (totalProb - 1.0f > 0.001f) {
            throw new IllegalStateException("Sum of probabilities of operations invalid: " + totalProb);
        }

        for (AbstractOperation op : m_operations) {
            op.init(p_memory, (long) (p_totalNumOperations * op.getProbability()));
        }

        m_threadsRunning = new AtomicInteger(0);
    }

    public String getName() {
        return m_name;
    }

    public void execute() {
        m_threads = new Thread[m_numThreads];

        for (int i = 0; i < m_threads.length; i++) {
            m_threads[i] = new Thread(i, m_operations, m_delayNsBetweenOps, m_threadsRunning);
        }

        m_threadsRunning.set(m_numThreads);

        long startTime = System.nanoTime();

        for (Thread thread : m_threads) {
            thread.start();
        }

        final long printIntervalNs = 1000 * 1000 * 1000;
        long totalTime = 0;
        long printTime = System.nanoTime();
        long prevOpsExecuted = 0;

        CpuProgress cpuProgress = new CpuProgress();
        MemState memoryState = new MemState();

        System.out.println("Benchmark running");

        while (m_threadsRunning.get() > 0) {
            long time = System.nanoTime();

            if (time - printTime >= printIntervalNs) {
                long opsExecuted = 0;

                for (Thread thread : m_threads) {
                    opsExecuted += thread.getProgressOperations();
                }

                totalTime += (time - printTime) / (1000 * 1000 * 1000);

                try {
                    cpuProgress.update();
                } catch (StateUpdateException e) {
                    System.out.println("Updating cpu progress failed: " + e);
                }

                try {
                    memoryState.update();
                } catch (StateUpdateException e) {
                    System.out.println("Updating memory state failed: " + e);
                }

                StringBuilder builder = new StringBuilder();

                builder.append(
                        String.format("[PROGRESS: %s] %d sec [OPS: Avg=%d, Perc=%.2f, Total=%d/%d]",
                                m_name, totalTime, opsExecuted - prevOpsExecuted,
                                ((double) opsExecuted / m_totalNumOperations) * 100, opsExecuted,
                                m_totalNumOperations));

                builder.append(
                        String.format("[CPU: Cur=%f][MEM: Used=%.2f, UsedMB=%.3f, FreeMB=%.3f]",
                                cpuProgress.getCpuUsagePercent(),
                                memoryState.getUsedPercent(),
                                memoryState.getUsed().getMBDouble(),
                                memoryState.getFree().getMBDouble()));

                System.out.println(builder);

                printTime = time;
                prevOpsExecuted = opsExecuted;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {

            }
        }

        for (Thread thread : m_threads) {
            try {
                thread.join();
            } catch (InterruptedException ignored) {

            }
        }

        m_totalTimeNs = System.nanoTime() - startTime;

        System.out.println("\nBenchmark finished, post-processing results...");

        for (AbstractOperation op : m_operations) {
            op.finish();
        }
    }

    public void printResults() {
        StringBuilder builder = new StringBuilder();

        builder.append("[OVERALL],BenchmarkPhase,");
        builder.append(m_name);
        builder.append('\n');
        builder.append("[OVERALL],RunTime(ms),");
        builder.append(m_totalTimeNs / 1000.0 / 1000.0);

        for (AbstractOperation op : m_operations) {
            builder.append('\n');
            builder.append(op.parameterToString());
        }

        for (Thread t : m_threads) {
            builder.append('\n');
            builder.append(t.parameterToString());
        }

        System.out.println(builder);
    }

    private static final class Thread extends java.lang.Thread {
        private final int m_id;
        private final AbstractOperation[] m_operations;
        private final long m_delayNsBetweenOps;
        private final AtomicInteger m_threadsRunning;

        private final long[] m_opCountExecuted;
        private final TimePercentile[] m_threadLocalTimePercentiles;

        private final AtomicLong m_progressOperations;

        private Thread(final int p_id, final AbstractOperation[] p_operations, final long p_delayNsBetweenOps,
                final AtomicInteger p_threadsRunning) {
            m_id = p_id;
            m_operations = p_operations;
            m_delayNsBetweenOps = p_delayNsBetweenOps;
            m_threadsRunning = p_threadsRunning;

            m_opCountExecuted = new long[m_operations.length];
            m_threadLocalTimePercentiles = new TimePercentile[m_operations.length];

            m_progressOperations = new AtomicLong(0);
        }

        public long getProgressOperations() {
            return m_progressOperations.get();
        }

        String parameterToString() {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < m_operations.length; i++) {
                builder.append("[Thread-");
                builder.append(m_id);
                builder.append("][");
                builder.append(m_operations[i].getName());
                builder.append("],TotalTime(ms),");
                builder.append(m_threadLocalTimePercentiles[i].getTotalValue(Time.Prefix.MILLI));
                builder.append('\n');
                builder.append("[Thread-");
                builder.append(m_id);
                builder.append("][");
                builder.append(m_operations[i].getName());
                builder.append("],OperationCount,");
                builder.append(m_threadLocalTimePercentiles[i].getCounter());
                builder.append('\n');
                builder.append("[Thread-");
                builder.append(m_id);
                builder.append("][");
                builder.append(m_operations[i].getName());
                builder.append("],LatencyAvg(us),");
                builder.append(m_threadLocalTimePercentiles[i].getAvg(Time.Prefix.MICRO));
                builder.append('\n');
                builder.append("[Thread-");
                builder.append(m_id);
                builder.append("][");
                builder.append(m_operations[i].getName());
                builder.append("],LatMin(us),");
                builder.append(m_threadLocalTimePercentiles[i].getMin(Time.Prefix.MICRO));
                builder.append('\n');
                builder.append("[Thread-");
                builder.append(m_id);
                builder.append("][");
                builder.append(m_operations[i].getName());
                builder.append("],LatMax(us),");
                builder.append(m_threadLocalTimePercentiles[i].getMax(Time.Prefix.MICRO));
                builder.append('\n');
                builder.append("[Thread-");
                builder.append(m_id);
                builder.append("][");
                builder.append(m_operations[i].getName());
                builder.append("],Lat95(us),");
                builder.append(m_threadLocalTimePercentiles[i].getPercentileScore(0.95f, Time.Prefix.MICRO));
                builder.append('\n');
                builder.append("[Thread-");
                builder.append(m_id);
                builder.append("][");
                builder.append(m_operations[i].getName());
                builder.append("],Lat99(us),");
                builder.append(m_threadLocalTimePercentiles[i].getPercentileScore(0.99f, Time.Prefix.MICRO));
                builder.append('\n');
                builder.append("[Thread-");
                builder.append(m_id);
                builder.append("][");
                builder.append(m_operations[i].getName());
                builder.append("],Lat999(us),");
                builder.append(m_threadLocalTimePercentiles[i].getPercentileScore(0.999f, Time.Prefix.MICRO));

                if (i + 1 < m_operations.length) {
                    builder.append('\n');
                }
            }

            return builder.toString();
        }

        @Override
        public void run() {
                 m_progressOperations.set(0);

            while (true) {
                if (m_delayNsBetweenOps > 0) {
                    LockSupport.parkNanos(m_delayNsBetweenOps);
                }

                float opProbability = (float) Math.random();
                int opSelected = -1;

                // select op
                for (int i = 0; i < m_operations.length; i++) {
                    if (opProbability < m_operations[i].getProbability()) {
                        if (m_operations[i].consumeOperation()) {
                            opSelected = i;
                            break;
                        }
                    } else {
                        opProbability -= m_operations[i].getProbability();
                    }
                }

                if (opSelected != -1) {
                    // execute in batches
                    for (int j = 0; j < m_operations[opSelected].getBatchCount(); j++) {
                        ChunkState state = m_operations[opSelected].execute();

                        if (state == ChunkState.OK) {
                            m_operations[opSelected].incSuccessful();
                        } else {
                            m_operations[opSelected].incError();
                        }

                        m_opCountExecuted[opSelected]++;
                    }

                    m_progressOperations.incrementAndGet();
                } else {
                    // no more ops left
                    break;
                }
            }

            // sort thread local values for per thread results
            for (int i = 0; i < m_operations.length; i++) {
                m_operations[i].getTimePercentile().getThreadLocal().sortValues();
                m_threadLocalTimePercentiles[i] = m_operations[i].getTimePercentile().getThreadLocal();
            }

            m_threadsRunning.decrementAndGet();
        }
    }
}
