package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.pt.PerfTimer;
import de.hhu.bsinfo.dxutils.FastByteUtils;
import de.hhu.bsinfo.dxutils.eval.MeasurementHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Florian Hucke (florian.hucke@hhu.de) on 06.03.18
 * @projectname dxram-memory
 */
@SuppressWarnings("unused")
public class MemoryEvaluation {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MemoryEvaluation.class.getSimpleName());
    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd_-_HH-mm-ss");

    private final MemoryManager memory;
    private final String resultFolder;

    private String fileNameExtension;
    private final char delim = ',';

    private int rounds = 3;
    private int threads = Runtime.getRuntime().availableProcessors();

    //heap init
    private final int initialChunks;
    private final int initMinSize;
    private final int initMaxSize;
    private final int[] initSizes;

    //delay init
    private long minDelay = 0;
    private long maxDelay = 0;

    //operations init
    private long operations = (long) Math.pow(10,8);

    //pinning
    private long[] entries;


    //Variable for multi threading
    //Prevent Java heap exceptions by too many Runnables
    private final LinkedBlockingQueue<Runnable> runnables= new LinkedBlockingQueue<>(100000);
    private final RejectedExecutionHandler rejectedExecutionHandler = (runnable, threadPoolExecutor) -> {
        if (!threadPoolExecutor.isShutdown()) {
            //wait until runnable is added
            do {
                if (threadPoolExecutor.getQueue().offer(runnable))
                    break;

            } while (true);

        }
    };

    /**
     * Constructor
     *
     * @param p_memoryManager
     *          The memory unit
     * @param p_resultPath
     *          The path for  measurement results
     * @param p_initialChunks
     *          Number of initial chunks
     * @param p_initMinSize
     *          Minimum chunk size at initialization
     * @param p_initMaxSize
     *          Maximum chunk size at initialization
     */
    public MemoryEvaluation(final MemoryManager p_memoryManager, final String p_resultPath,
                            final int p_initialChunks, final int p_initMinSize, final int p_initMaxSize) {
        memory = p_memoryManager;

        initialChunks = p_initialChunks;
        initMinSize = p_initMinSize;
        initMaxSize = p_initMaxSize;

        initSizes = new int[p_initialChunks];
        for (int i = 0; i < initSizes.length; i++) {
            initSizes[i] = (int)getRandom(p_initMinSize, p_initMaxSize);
        }

        String tmpPath =  p_resultPath;
        if(!tmpPath.endsWith("/"))
            tmpPath += "/";

        tmpPath += df.format(new Date()) + "/";

        resultFolder = tmpPath;
    }


    public void setLocks(final boolean readLock, final boolean writeLock, final boolean disableReadLock, final boolean disableWriteLock, final int waitHandle) {
        memory.setLocks(readLock, writeLock);
        memory.disableReadLock(disableReadLock);
        memory.disableWriteLock(disableWriteLock);
        memory.cidTable.setThreadWaitHandle(waitHandle);

        if(disableReadLock && disableWriteLock)
            fileNameExtension = "no_locks";
        else if (disableReadLock)
            fileNameExtension = String.format("write_%s_-_no_read_lock", (writeLock) ? "w":"r");
        else if (disableWriteLock)
            fileNameExtension = String.format("read_%s_-_no_write_lock", (readLock) ? "w":"r");
        else
            fileNameExtension = String.format("read_%s_-_write_%s", (readLock) ? "r":"w", (writeLock) ? "w":"r");

        if(waitHandle != 0 && waitHandle < 3) {
            fileNameExtension += "_" + ((waitHandle==1) ? "yield":"parkNanos");
        }

    }


    /**
     *
     * @param p_operations
     *          Number of operations
     */
    public void setOperations(final long p_operations) {
        operations = p_operations;
    }

    /**
     * Set delay between two operations
     *
     * @param p_minDelay
     *          Minimum delay between operations
     * @param p_maxDelay
     *          Maximum delay between operations
     */
    public void setDelay(long p_minDelay, long p_maxDelay) {
        minDelay = p_minDelay;
        maxDelay = p_maxDelay;
    }

    /**
     * Set number of test rounds
     *
     * @param p_rounds
     *          Number of test runs
     */
    public void setRounds(int p_rounds) {
        rounds = p_rounds;
    }

    /**
     * Set number of threads
     *
     * @param p_threads
     *          Number of threads
     */
    public void setThreads(final int p_threads) {
        threads = p_threads;
    }

    /**
     * Init the heap
     *
     * @param pinning Is a pinning test?
     */
    private void initHeap(final boolean pinning) {
        final byte[] data = FastByteUtils.longToBytes(0);
        long cid = 1;

        //delete old chunks
        while (memory.info.numActiveChunks > 0) {
            memory.cidTable.setState(cid, CIDTableEntry.STATE_NOT_MOVEABLE, false);
            memory.management.remove(cid++, false);
        }

        if(pinning)
            entries = new long[initialChunks];

        //Create initial chunks
        for (int i = 0; i < initSizes.length; i++) {
            cid = memory.management.create(initSizes[i]);
            memory.access.put(cid, data);

            if(pinning)
                entries[i] = memory.pinning.pinChunk(cid);
        }
    }

    /**
     * Extended memory test to emulate real life access
     *
     * @param createProbability
     *          Probability of chunk creation
     * @param removeProbability
     *          Chunk deletion probability
     * @param writeProbability
     *          Probability of write access to a chunk
     * @param minSize
     *          Minimum size of a newly created chunk
     * @param maxSize
     *          Maximum size of a newly created chunk
     */
    public final void accessSimulation(final double createProbability, final double removeProbability,
                                       final double writeProbability, final int minSize, final int maxSize) {

        double removeLimit = createProbability + removeProbability;
        double writeLimit = removeLimit + writeProbability;

        String baseFilename = String.format(Locale.US, "%2.3f_%2.3f_%2.3f", createProbability, removeProbability, writeProbability);

        String desc = String.format(Locale.US, "operations: %d, threads: %d, init chunks: %d, inti size: [min: %d ,max: %d], " +
                "probabilities: [create: %2.3f, remove: %2.3f, read: %2.3f, write: %2.3f], delay:[min: %d, max: %d], size:[min: %d, max:%d]",
                operations, threads, initialChunks, initMinSize, initMaxSize, createProbability, removeProbability,
                1-writeLimit, writeProbability, minDelay, maxDelay, minSize, maxSize);

        //FunctionalInterface for incrementing the value (with a strong consistency)
        //ByteDataManipulation increment = (byte[] oldData) -> FastByteUtils.longToBytes(FastByteUtils.bytesToLong(oldData) + 1);

        AtomicLong putCounter = new AtomicLong(0);

        
        //Operation counter
        MeasurementHelper measurementHelper = new MeasurementHelper(resultFolder, baseFilename,  desc, false,
                "read", "write", "create", "remove");
        
        MeasurementHelper.Measurement read = measurementHelper.getMeasurement("read");
        MeasurementHelper.Measurement write = measurementHelper.getMeasurement("write");
        MeasurementHelper.Measurement create = measurementHelper.getMeasurement("create");
        MeasurementHelper.Measurement remove = measurementHelper.getMeasurement("remove");

        assert read != null && write != null && create != null && remove != null;

        final byte[] data = FastByteUtils.longToBytes(0);
        long cid;

        Runnable r = () -> {
            wait(minDelay, maxDelay);

            long randomCID = getRandom(1, memory.info.getHighestUsedLocalID());
            double selector = Math.random();
            long start;
            boolean ok;

            if(selector < createProbability) {
                //create
                start = SimpleStopwatch.startTime();
                ok = memory.management.create((int)getRandom(minSize, maxSize)) != ChunkID.INVALID_ID;
                create.addTime(ok, SimpleStopwatch.stopAndGetDelta(start));

            } else if(createProbability <= selector && selector < removeLimit) {
                start = SimpleStopwatch.startTime();
                ok = memory.management.remove(randomCID, false) != ChunkID.INVALID_ID;
                remove.addTime(ok, SimpleStopwatch.stopAndGetDelta(start));
            } else if(removeLimit <= selector && selector < writeLimit) {
                start = SimpleStopwatch.startTime();
                ok = memory.access.put(randomCID, FastByteUtils.longToBytes(putCounter.getAndIncrement()),true);
                write.addTime(ok, SimpleStopwatch.stopAndGetDelta(start));
            } else {
                //read data
                start = SimpleStopwatch.startTime();
                ok = memory.access.get(randomCID) != null;
                read.addTime(ok, SimpleStopwatch.stopAndGetDelta(start));
            }
        };

        for (int i = 0; i < rounds; i++) {
            initHeap(false);

            try {
                execNOperationsRunnables(threads, threads, operations, r);
            } catch (InterruptedException e) {
                System.err.println("Failed");
            }

            try {
                measurementHelper.writeStats(fileNameExtension, delim);
            } catch (IOException ignored) {

            }
            measurementHelper.newRound();
        }
        memory.disableReadLock(false);
    }

    /**
     * Extended memory test to emulate real life access
     *
     * @param writeProbability
     *          Probability of write access to a chunk
     */
    public final void accessSimulationPinning(final double writeProbability) {
        AtomicLong putCounter = new AtomicLong(0);

        String baseFilename = String.format(Locale.US, "%2.3f_%2.3f_%2.3f", 0.0, 0.0, writeProbability);

        String desc = String.format(Locale.US, "operations: %d, threads: %d, init chunks: %d, inti size: [min: %d ,max: %d], " +
                        "probabilities: [read: %2.3f, write: %2.3f]",
                operations, threads, initialChunks, initMinSize, initMaxSize, 1-writeProbability, writeProbability);

        //Operation counter
        MeasurementHelper measurementHelper = new MeasurementHelper(resultFolder, baseFilename,  desc, false,
                "read", "write");

        MeasurementHelper.Measurement read = measurementHelper.getMeasurement("read");
        MeasurementHelper.Measurement write = measurementHelper.getMeasurement("write");

        assert read != null && write != null;

        Runnable r = () -> {
            wait(minDelay, maxDelay);

            long randomAddress = entries[(int)getRandom(0, entries.length-1)];
            double selector = Math.random();
            long start;
            boolean ok;

            if(selector < writeProbability) {
                start = SimpleStopwatch.startTime();
                ok = memory.pinning.put(randomAddress, FastByteUtils.longToBytes(putCounter.getAndIncrement()));
                write.addTime(ok, SimpleStopwatch.stopAndGetDelta(start));
            } else {
                //read data
                start = SimpleStopwatch.startTime();
                ok = memory.pinning.get(randomAddress) != null;
                read.addTime(ok, SimpleStopwatch.stopAndGetDelta(start));
            }
        };

        initHeap(true);
        fileNameExtension = "pinning";

        for (int i = 0; i < rounds; i++) {
            try {
                execNOperationsRunnables(threads, threads, operations, r);
            } catch (InterruptedException e) {
                System.err.println("Failed");
            }

            try {
                measurementHelper.writeStats(fileNameExtension, delim);
            } catch (IOException ignored) {
            }

            measurementHelper.newRound();
        }

    }

    /**
     * Execute a given Runnable n-times
     *
     * @param coreThreads
     *          The minimal number of threads
     * @param maxThreads
     *          The maximal number of threads
     * @param operations
     *          Number of operations to run
     * @param runnable
     *          The runnable to run n-times
     * @throws InterruptedException
     *          Termination can throw this exception
     */
    private void execNOperationsRunnables(final int coreThreads, final int maxThreads, final long operations,
                                          final Runnable runnable) throws InterruptedException {
        ThreadPoolExecutor exec = new ThreadPoolExecutor(coreThreads, maxThreads,24, TimeUnit.HOURS,
                runnables, rejectedExecutionHandler);

        //start the threads
        for (long i = 0; i < operations; i++) {
            exec.execute(runnable);
        }

        //don't start new threads
        exec.shutdown();

        //wait until all threads a terminated
        while (!exec.awaitTermination(24L, TimeUnit.HOURS)) {
            LOGGER.info("Not yet. Still waiting for termination");
        }

    }

    /**
     * Execute a runnable in a time window again and again
     *
     * @param coreThreads
     *          The minimal number of threads
     * @param maxThreads
     *          The maximal number of threads
     * @param maxTime
     *          The time to run the Runnables
     * @param runnable
     *          The runnable to run again and again
     * @throws InterruptedException
     *          Termination can throw this exception
     */
    private void execMaxTimeRunnables(final int coreThreads, final int maxThreads, final long maxTime,
                                      final Runnable runnable) throws InterruptedException {
        long stopTime = System.currentTimeMillis() + maxTime;

        ThreadPoolExecutor exec = new ThreadPoolExecutor(coreThreads, maxThreads,24, TimeUnit.HOURS,
                runnables, rejectedExecutionHandler);

        //start the threads
        while (System.currentTimeMillis() < stopTime) {
            exec.execute(runnable);
        }

        //don't start new threads
        exec.shutdown();

        //wait until all threads a terminated
        while (!exec.awaitTermination(24L, TimeUnit.HOURS)) {
            LOGGER.info("Not yet. Still waiting for termination");
        }
    }

    /**
     * Wait a random time
     *
     * @param minValue
     *          Minimal time to wait
     * @param maxValue
     *          Maximal time to wait
     */
    private void wait(final long minValue, final long maxValue){
        try {
            Thread.sleep(getRandom(minValue, maxValue));
        } catch (InterruptedException ignored) {}
    }

    /**
     * Get a random number in the range [minValue, maxValue]
     *
     * @param minValue
     *          Minimal number
     * @param maxValue
     *          Maximal number
     * @return A random number of [minValue, maxValue]
     */
    private long getRandom(long minValue, long maxValue){
        return minValue + (long)(Math.random() * (maxValue - minValue));
    }

    /**
     * Simple Stopwatch based on the PerfTimer
     */
    public static class SimpleStopwatch {
        static {
            PerfTimer.init(PerfTimer.Type.SYSTEM_NANO_TIME);
        }

        /**
         * Get the current time as start time
         *
         * @return Start time
         */
        static long startTime() {
            return PerfTimer.start();
        }

        /**
         * Get a time delta
         *
         * @param startTime
         *          The start time
         * @return Time delta
         */
        static long stopAndGetDelta(long startTime){
            return PerfTimer.convertToNs(PerfTimer.considerOverheadForDelta(PerfTimer.endWeak() - startTime));
        }
    }

}
