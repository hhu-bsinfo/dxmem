package de.hhu.bsinfo.dxmem.benchmark;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.benchmark.operation.AbstractOperation;
import de.hhu.bsinfo.dxmem.core.CIDTableStatus;
import de.hhu.bsinfo.dxmem.core.HeapStatus;
import de.hhu.bsinfo.dxmem.core.LIDStoreStatus;
import de.hhu.bsinfo.dxmem.core.MemoryOverheadCalculator;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxmonitor.progress.CpuProgress;
import de.hhu.bsinfo.dxmonitor.state.MemState;
import de.hhu.bsinfo.dxmonitor.state.StateUpdateException;
import de.hhu.bsinfo.dxutils.stats.Time;
import de.hhu.bsinfo.dxutils.stats.TimePercentile;

public class BenchmarkPhase {
    private final String m_name;
    private final DXMem m_memory;
    private final int m_numThreads;
    private final long m_totalNumOperations;
    private final long m_delayNsBetweenOps;
    private final AbstractOperation[] m_operations;

    private Thread[] m_threads;
    private long m_totalTimeNs;
    private double m_aggregatedOpsPerSec;

    public BenchmarkPhase(final String p_name, final DXMem p_memory, final int p_numThreads,
            final long p_totalNumOperations, final long p_delayNsBetweenOps, final AbstractOperation... p_operations) {
        m_name = p_name;
        m_memory = p_memory;
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
    }

    public String getName() {
        return m_name;
    }

    public void execute(final ChunkIDRanges p_cidRanges, final ReentrantLock p_cidRangesLock) {
        // init ops
        for (AbstractOperation op : m_operations) {
            op.init(m_memory, p_cidRanges, p_cidRangesLock, (long) (m_totalNumOperations * op.getProbability()));
        }

        AtomicInteger threadsRunning = new AtomicInteger(0);

        m_threads = new Thread[m_numThreads];

        for (int i = 0; i < m_threads.length; i++) {
            m_threads[i] = new Thread(i, m_operations, m_delayNsBetweenOps, threadsRunning);
        }

        threadsRunning.set(m_numThreads);

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

        while (threadsRunning.get() > 0) {
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

                HeapStatus heapStatus = m_memory.stats().getHeapStatus();
                CIDTableStatus cidTableStatus = m_memory.stats().getCIDTableStatus();
                LIDStoreStatus lidStoreStatus = m_memory.stats().getLIDStoreStatus();

                StringBuilder builder = new StringBuilder();

                builder.append(
                        String.format("[PROGRESS: %s] %d sec [OPS: Perc=%.2f, Total=%d/%d]",
                                m_name, totalTime, ((double) opsExecuted / m_totalNumOperations) * 100,
                                opsExecuted, m_totalNumOperations));

                builder.append(
                        String.format("[MEMOVERHEAD: Perc=%.2f]",
                                MemoryOverheadCalculator.calculate(heapStatus, cidTableStatus) * 100.f));

                builder.append(
                        String.format("[HEAP: TotalMB=%f, FreeMB=%f, UsedMB=%f, AllocPayloadMB=%f, AllocBlocks=%d, " +
                                "FreeBlocks=%d, FreeSmallBlocks=%d]",
                                        heapStatus.getTotalSize().getMBDouble(),
                                        heapStatus.getFreeSize().getMBDouble(),
                                        heapStatus.getUsedSize().getMBDouble(),
                                        heapStatus.getAllocatedPayload().getMBDouble(),
                                        heapStatus.getAllocatedBlocks(),
                                        heapStatus.getFreeBlocks(),
                                        heapStatus.getFreeSmall64ByteBlocks()));

                builder.append(
                        String.format("[CIDT: TableCount=%d, Level3=%d, Level2=%d, Level1=%d, Level0=%d, " +
                                "TableMemoryMB=%f]",
                                        cidTableStatus.getTotalTableCount(),
                                        cidTableStatus.getTableCountOfLevel(3),
                                        cidTableStatus.getTableCountOfLevel(2),
                                        cidTableStatus.getTableCountOfLevel(1),
                                        cidTableStatus.getTableCountOfLevel(0),
                                        cidTableStatus.getTotalPayloadMemoryTables().getMBDouble()));

                builder.append(
                        String.format("[LIDS: Counter=%d, TotalFree=%d, FreeStore=%d]",
                                lidStoreStatus.getCurrentLIDCounter(),
                                lidStoreStatus.getTotalFreeLIDs(),
                                lidStoreStatus.getTotalLIDsInStore()));

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

        for (Thread t : m_threads) {
            m_aggregatedOpsPerSec += t.getAggregatedThroughputOpsSec();
        }

        m_totalTimeNs = System.nanoTime() - startTime;

        System.out.println("\nBenchmark finished, post-processing results...");

        for (AbstractOperation op : m_operations) {
            op.finish();
        }
    }

    public void printResults() {
        StringBuilder builder = new StringBuilder();

        HeapStatus heapStatus = m_memory.stats().getHeapStatus();
        CIDTableStatus cidTableStatus = m_memory.stats().getCIDTableStatus();
        LIDStoreStatus lidStoreStatus = m_memory.stats().getLIDStoreStatus();

        builder.append("[OVERALL],BenchmarkPhase,");
        builder.append(m_name);
        builder.append('\n');

        builder.append("[OVERALL],RunTime(ms),");
        builder.append(m_totalTimeNs / 1000.0 / 1000.0);
        builder.append('\n');

        builder.append("[OVERALL],AggregatedThroughput(mops/sec),");
        builder.append(m_aggregatedOpsPerSec / 1000000);
        builder.append('\n');

        builder.append("[OVERALL],MemoryMetadataOverhead(%),");
        builder.append(MemoryOverheadCalculator.calculate(heapStatus, cidTableStatus) * 100.f);
        builder.append('\n');

        // sum up operation return codes of all ops
        long[] opReturnCodes = new long[ChunkState.values().length];

        for (AbstractOperation op : m_operations) {
            for (int i = 0; i < ChunkState.values().length; i++) {
                opReturnCodes[i] += op.getNumReturnCodes(ChunkState.values()[i]);
            }
        }

        for (int i = 0; i < ChunkState.values().length; i++) {
            builder.append("[OVERALL],OperationReturnCode(");
            builder.append(ChunkState.values()[i]);
            builder.append("),");
            builder.append(opReturnCodes[i]);
            builder.append('\n');
        }

        builder.append("[HEAP],Total(mb),");
        builder.append(heapStatus.getTotalSize().getMBDouble());
        builder.append('\n');

        builder.append("[HEAP],Free(mb),");
        builder.append(heapStatus.getFreeSize().getMBDouble());
        builder.append('\n');

        builder.append("[HEAP],Used(mb),");
        builder.append(heapStatus.getUsedSize().getMBDouble());
        builder.append('\n');

        builder.append("[HEAP],AllocPayload(mb),");
        builder.append(heapStatus.getAllocatedPayload().getMBDouble());
        builder.append('\n');

        builder.append("[HEAP],AllocBlocks,");
        builder.append(heapStatus.getAllocatedBlocks());
        builder.append('\n');

        builder.append("[HEAP],FreeBlocks,");
        builder.append(heapStatus.getFreeBlocks());
        builder.append('\n');

        builder.append("[HEAP],FreeSmall64ByteBlocks,");
        builder.append(heapStatus.getFreeSmall64ByteBlocks());
        builder.append('\n');

        builder.append("[CIDTable],TotalTableCount,");
        builder.append(cidTableStatus.getTotalTableCount());
        builder.append('\n');

        builder.append("[CIDTable],TablesLevel3,");
        builder.append(cidTableStatus.getTableCountOfLevel(3));
        builder.append('\n');

        builder.append("[CIDTable],TablesLevel2,");
        builder.append(cidTableStatus.getTableCountOfLevel(2));
        builder.append('\n');

        builder.append("[CIDTable],TablesLevel1,");
        builder.append(cidTableStatus.getTableCountOfLevel(1));
        builder.append('\n');

        builder.append("[CIDTable],TablesLevel0,");
        builder.append(cidTableStatus.getTableCountOfLevel(0));
        builder.append('\n');

        builder.append("[CIDTable],TableMemoryPayload(mb),");
        builder.append(cidTableStatus.getTotalPayloadMemoryTables().getMBDouble());
        builder.append('\n');

        builder.append("[LIDStore],CurrentLIDCounter,");
        builder.append(lidStoreStatus.getCurrentLIDCounter());
        builder.append('\n');

        builder.append("[LIDStore],TotalFreeLIDs,");
        builder.append(lidStoreStatus.getTotalFreeLIDs());
        builder.append('\n');

        builder.append("[LIDStore],TotalLIDsInStore,");
        builder.append(lidStoreStatus.getTotalLIDsInStore());
        builder.append('\n');

        for (AbstractOperation op : m_operations) {
            builder.append('\n');
            builder.append(op.parameterToString());
        }

        builder.append('\n');

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
            super("Benchmark-" + p_id);

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

        public double getAggregatedThroughputOpsSec() {
            double tp = 0.0;

            for (TimePercentile t : m_threadLocalTimePercentiles) {
                if (t.getCounter() > 0) {
                    tp += t.getCounter() / t.getTotalValue(Time.Prefix.SEC);
                }
            }

            return tp;
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
                builder.append("],Throughput(mops/sec),");
                builder.append(m_threadLocalTimePercentiles[i].getCounter() /
                        m_threadLocalTimePercentiles[i].getTotalValue(Time.Prefix.SEC) / 1000000);
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
                        ChunkState state = ChunkState.UNDEFINED;

                        try {
                            state = m_operations[opSelected].execute();
                        } catch (final Exception e) {
                            // abort benchmark on critical errors
                            System.out.println("ERROR: " + e.getMessage());
                            e.printStackTrace();
                            System.exit(-1);
                        }

                        m_operations[opSelected].incReturnCode(state);

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
