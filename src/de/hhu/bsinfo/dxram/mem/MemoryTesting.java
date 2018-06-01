package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.mem.manipulation.ByteDataManipulation;
import de.hhu.bsinfo.dxram.mem.manipulation.ChunkDataManipulationTesting;
import de.hhu.bsinfo.dxutils.FastByteUtils;
import de.hhu.bsinfo.dxutils.eval.Stopwatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A unit to test all the implementations
 * 
 * @author Florian Hucke (florian.hucke@hhu.de) on 13.02.18
 * @projectname dxram-memory
 */
@SuppressWarnings("unused")
public final class MemoryTesting {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MemoryTesting.class.getSimpleName());

    private final MemoryManager memory;

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

    private static int chunkSize = -1;
    private static int nChunks = -1;
    private long[] cids;
    private long[] ref;
    private AtomicLong[] counter;
    private AtomicLong countOperations;

    /**
     * Create a testing instance
     */
    public MemoryTesting(final MemoryManager memoryManager){
        memory = memoryManager;
    }

    /**
     * Get the used MemoryManager
     *
     * @return the current used MemoryManager
     */
    public final MemoryManager getMemoryManager() {
        return memory;
    }

    /**
     * delete all chunks
     */
    public final void resetHeap(){
        for (int i = 0; i <= memory.info.getHighestUsedLocalID(); i++) {
            memory.management.remove(i, false);
        }
    }

    /**
     * Destroy the testing instance
     */
    public final void destroy(){
        memory.shutdownMemory();
    }

    /**
     * Execute a given Runnable n-times
     *
     * @param coreThreads The minimal number of threads
     * @param maxThreads The maximal number of threads
     * @param operations Number of operations to run
     * @param runnable The runnable to run n-times
     * @throws InterruptedException Termination can throw this exception
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
     * @param coreThreads The minimal number of threads
     * @param maxThreads The maximal number of threads
     * @param maxTime The time to run the Runnables
     * @param runnable The runnable to run again and again
     * @throws InterruptedException Termination can throw this exception
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
     * Initialize a heap of chunks that are all of identical size
     *
     * @param p_chunkSize The size of the chunks
     * @param p_nChunks The number of chunks
     */
    public final void initHeap(final int p_chunkSize, final int p_nChunks){
        chunkSize = p_chunkSize;
        nChunks = p_nChunks;

        counter = new AtomicLong[nChunks];
        cids = memory.management.createMulti(p_chunkSize, p_nChunks);
        ref = new long[nChunks];

        for (int i = 0; i < nChunks; i++) {
            memory.access.put(cids[i], FastByteUtils.longToBytes(0));

            ref[i] = 0;
            counter[i] = new AtomicLong(0);
        }
        countOperations = new AtomicLong(0);
    }

    /**
     * Reset all counter, but keep the chunks
     */
    public final void reInitHeap(){
        //reset values
        for (int i = 0; i < nChunks; i++) {
            memory.access.put(cids[i], FastByteUtils.longToBytes(0));
            ref[i] = 0;
            counter[i].set(0);
        }

        countOperations.set(0);
    }

    /**
     * Reset the counter and add chunks to the MemoryComponent
     *
     * @param chunksToAdd The number of chunks to add
     */
    public final void addChunkAndReset(final int chunksToAdd){
        long[] newCIDS = new long[cids.length + chunksToAdd];
        System.arraycopy(cids, 0, newCIDS, 0, cids.length);

        for (int i = cids.length; i < newCIDS.length; i++) {
            newCIDS[i] = memory.management.create(chunkSize);
        }

        cids = newCIDS;
        nChunks = cids.length;

        ref = new long[nChunks];
        counter = new AtomicLong[nChunks];

        for (int i = 0; i < nChunks; i++) {
            memory.access.put(cids[i], FastByteUtils.longToBytes(0));
            ref[i] = 0;
            counter[i] = new AtomicLong(0);
        }

        countOperations.set(0);
    }

    /**
     * Multi-Threaded locking test with n-chunks, with equal chunk size,
     * This test is considered as a functionality test of the locks.
     *
     * @param nOperations Operation count
     * @param nThreads Active threads at the same time
     * @param writeProbability Probability of a write access
     */
    public final void lockTestFunctionality(final long nOperations, final int nThreads, final double writeProbability) {
        assert nChunks > 0: "Run initHeap(final int p_chunkSize, final int p_nChunks) first";
        ChunkDataManipulationTesting increment = (oldData, selected) -> {
            if(ref[selected] != FastByteUtils.bytesToLong(oldData)){
                LOGGER.error("write expected: " + ref[selected] + " get: " + FastByteUtils.bytesToLong(oldData));
                System.exit(3);
            } else {
                ref[selected]++;
            }
            counter[selected].getAndIncrement();
            return FastByteUtils.longToBytes(FastByteUtils.bytesToLong(oldData) + 1);
        };

        //Lambda Runnable
        Runnable r = () -> {
            //select chunk
            int selectedChunk = (int) getRandom(0, cids.length);

            if(Math.random() <= writeProbability){
                //write access
                byte[] data = memory.access.get(cids[selectedChunk], true);
                ref[selectedChunk]++;
                counter[selectedChunk].incrementAndGet();
                data = FastByteUtils.longToBytes(FastByteUtils.bytesToLong(data)+1);
                memory.access.put(cids[selectedChunk], data, false);
            } else{
                //read access
                long tmp = FastByteUtils.bytesToLong(memory.access.getTesting(cids[selectedChunk], ref, selectedChunk));
            }
        };

        //Perform n operations with the Runnable
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        try {
            execNOperationsRunnables(nThreads, nThreads, nOperations, r);
        } catch (InterruptedException ignored) {

        }
        stopwatch.stop();

        TestingMeasurements.add(nThreads, stopwatch.toString());

        //check if the counter equals the storage value
        for (int i = 0; i < nChunks; i++) {
            if (counter[i].get() != FastByteUtils.bytesToLong(memory.access.get(cids[i]))) {
                LOGGER.error("expected: " + counter[i].get() + " get: " + FastByteUtils.bytesToLong(memory.access.get(cids[i])));
                System.exit(1);
            }
        }

        LOGGER.info("Run: %d ALL OK => %s", TestingMeasurements.l.size(), TestingMeasurements.l.getLast());
    }

    /**
     * Multi-Threaded locking test with n-chunks, with equal chunk size.
     * This test assumes that the locks work and only tests the speed.
     *
     * @param nOperations Operation count
     * @param nThreads Active threads at the same time
     * @param writeProbability Probability of a write access
     * @throws InterruptedException Termination can throw this exception
     */
    public final void lockTestSpeed(final long nOperations, final int nThreads, final double writeProbability) throws InterruptedException {
        assert nChunks > 0: "Run initHeap(final int p_chunkSize, final int p_nChunks) first";

        ByteDataManipulation increment = (byte[] oldData) -> FastByteUtils.longToBytes(FastByteUtils.bytesToLong(oldData) + 1);

        //Lambda Runnable
        Runnable r = () -> {
            //select chunk
            int selectedChunk = (int) getRandom(0, cids.length);

            if(Math.random() <= writeProbability){
                //write access
                memory.access.modify(cids[selectedChunk], increment);
            } else{
                byte[] test = memory.access.get(cids[selectedChunk]);
                long tmp = FastByteUtils.bytesToLong(test);
            }
        };

        //Perform n operations with the Runnable
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        execNOperationsRunnables(nThreads, nThreads, nOperations, r);
        stopwatch.stop();

        TestingMeasurements.add(nThreads, stopwatch.toString());

        LOGGER.info("Run: %d ALL OK => %s", TestingMeasurements.l.size(), TestingMeasurements.l.getLast());
    }

    /**
     * Create String as data
     *
     * @param nOperations How many string we want create
     * @param strings String we want to write
     */
    public final void createAndWriteStringObjects(final long nOperations, final int nThreads, final String[] strings, final boolean testData) throws InterruptedException {
        boolean delete = false;

        //Runnable
        Runnable r = () -> {
            //get random String
            String str = strings[(int)getRandom(0, strings.length - 1)];
            long cid = memory.management.create(str.length());
             memory.access.put(cid, str.getBytes());
        };

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        execNOperationsRunnables(nThreads, nThreads, nOperations, r);
        stopwatch.stop();
        TestingMeasurements.add(nThreads, stopwatch.toString());

        if(testData){
            //In a lambda Runnable we can only work with final variables.
            //Therefore, i becomes a quasi-final variable by using an array.
            for (int[] i = {0}; i[0] <= memory.info.getHighestUsedLocalID(); i[0]++) {
                byte[] data = memory.access.get(i[0]);
                if(data != null){
                    if (Arrays.stream(strings).noneMatch(x -> x.matches(new String(data)))) {
                        LOGGER.error("Error >>" + new String(data));
                        return;
                    }
                }

                if(delete)
                    memory.management.remove(i[0], false);

                delete = !delete;

            }
        }

        LOGGER.info("Run: %d => %s", TestingMeasurements.l.size(), TestingMeasurements.l.getLast());
    }

    /**
     * Test if pinning is functional
     *
     * @param nChunks Chunks we want to test.
     */
    public void pinningFunctional(int nChunks, int blockSize){
        byte[] data = new byte[blockSize];

        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        long[] cids = memory.management.createMulti(blockSize,nChunks);
        long[] entry = new long[nChunks];

        for (int i = 0; i < nChunks; i++) {
            entry[i] = memory.pinning.pinChunk(cids[i]); //Pinning ok?
            memory.management.remove(cids[i], false); //Flag ok?

            memory.pinning.put(entry[i], data); //access.put ok?
            byte[] testRead = memory.pinning.get(entry[i]); //get ok?
            for (int j = 0; j < data.length; j++) { //check if really ok?
                if(data[j] != testRead[j]) {
                    System.exit(-1);
                }
            }

            if(cids[i] != memory.pinning.unpinChunk(entry[i])){ //do unpin
                System.out.println("DFS error CID: " + cids[i]);
                System.exit(-1);
            }
            memory.management.remove(cids[i], true); //flag ok?
        }
    }

    /**
     * Extended memory test to emulate real life access
     *
     * @param nOperations
     *          Operation count
     * @param nThreads
     *          Number of Threads
     * @param initialChunks
     *          The initial chunks
     * @param initMinSize
     *          Minimal initial chunk size
     * @param initMaxSize
     *          Maximal initial chunk size
     * @param createProbability
     *          Probability of a create access (complement is a delete access)
     * @param readProbability
     *          Probability of a read access (complement is a write access)
     * @param changeProbability
     *          Probability of a data change (complement is a create/delete access)
     * @param minDelayInMS
     *          Minimal delay between operations
     * @param maxDelay
     *          Maximal delay between operations
     * @param minSize
     *          Minimal byte size for a object
     * @param maxSizeInByte
     *          Maximal byte size for a object
     */
    public final void memoryManagementTest(final long nOperations, final int nThreads, final long initialChunks,
                                           final int initMinSize, final int initMaxSize,
                                           final double createProbability, final double readProbability,
                                           final double changeProbability, final long minDelayInMS, final long maxDelay,
                                           final int minSize, final int maxSizeInByte) throws InterruptedException {

        //FunctionalInterface for incrementing the value (with a strong consistency)
        ByteDataManipulation increment = (byte[] oldData) -> FastByteUtils.longToBytes(FastByteUtils.bytesToLong(oldData) + 1);

        //Operation counter
        final AtomicLong runs = new AtomicLong(0);
        final AtomicLong read = new AtomicLong(0);
        final AtomicLong write = new AtomicLong(0);
        final AtomicLong create = new AtomicLong(0);
        final AtomicLong delete = new AtomicLong(0);

        Runnable r = () -> {
            runs.incrementAndGet();
            wait(minDelayInMS, maxDelay);

            long cid = getRandom(1, memory.info.getHighestUsedLocalID());
            if(Math.random() < changeProbability){
                if(Math.random() < readProbability){
                    //read data
                    if(memory.access.get(cid) != null)
                        read.incrementAndGet();

                } else {
                    //change data
                    if(memory.access.modify(cid, increment))
                        write.incrementAndGet();
                }
            } else {
                if(Math.random() < createProbability){
                    //create
                    memory.management.create((int)getRandom(minSize, maxSizeInByte));
                    create.incrementAndGet();
                } else{
                    if(memory.management.remove(cid, false) != -1){
                        delete.incrementAndGet();
                    }
                }
            }
        };

        //Create initial chunks
        byte[] data = FastByteUtils.longToBytes(0);
        long cid;
        for (int i = 0; i < initialChunks; i++) {
            cid = memory.management.create((int)getRandom(initMinSize, initMaxSize));
            memory.access.put(cid, data);
        }

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        execNOperationsRunnables(nThreads, nThreads, nOperations, r);
        stopwatch.stop();

        TestingMeasurements.add(nThreads, stopwatch.toString());

        LOGGER.info(String.format("Operations: %d [read: %d, write %d, create: %d, delete: %d]",
                runs.get(), read.get(), write.get(), create.get(), delete.get()));
        LOGGER.info("Run: %d => %s", TestingMeasurements.l.size(), TestingMeasurements.l.getLast());
    }

    /**
     * Emulate only create and delete accesses
     *  @param nOperations
     *          Operation count
     * @param createProbability
     *          Probability of a chunk creation
     * @param minSize
 *              Minimal size of a chunk
     * @param maxSize
     *          Maximal size of a chunk
     */
    public final void createDeleteTest(final long nOperations, final int nThreads, final double createProbability, final int minSize, final int maxSize) {
        AtomicLong readCount = new AtomicLong(0);
        AtomicLong writeCount = new AtomicLong(0);
        //Create a Runnable
        Runnable r = () -> {
            wait(0L,0L);

            if(Math.random() < createProbability){
                int size = (int)getRandom(minSize, maxSize);
                memory.management.create(size);
                readCount.incrementAndGet();

            } else{
                long remove = getRandom(1, memory.info.getHighestUsedLocalID());
                if(memory.management.remove(remove, false) != -1){
                    writeCount.incrementAndGet();
                }
            }

        };

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        try {
            execNOperationsRunnables(nThreads, nThreads, nOperations, r);
        } catch (InterruptedException ignored) {}
        stopwatch.stop();

        TestingMeasurements.add(nThreads, stopwatch.toString());

        LOGGER.info("reads: %d, writes: %d", readCount.get(), writeCount.get());
        LOGGER.info("Run: %d => %s", MemoryTesting.TestingMeasurements.l.size(), MemoryTesting.TestingMeasurements.l.getLast());

    }

    /**
     * Wait a random time
     *
     * @param minValue Minimal time to wait
     * @param maxValue Maximal time to wait
     */
    private void wait(final long minValue, final long maxValue){
        try {
            Thread.sleep(getRandom(minValue, maxValue));
        } catch (InterruptedException ignored) {

        }
    }

    /**
     * Get a random number in the range [minValue, maxValue]
     *
     * @param minValue Minimal number
     * @param maxValue Maximal number
     * @return A random number of [minValue, maxValue]
     */
    private long getRandom(long minValue, long maxValue){
        return minValue + (long)(Math.random() * (maxValue - minValue));
    }

    /**
     * Handle Measurements
     */
    public static class TestingMeasurements {
        static LinkedList<String> l = new LinkedList<>();

        private static void add(int threads, String timeStr){
            l.addLast(String.format("Threads: %d, Chunks: %d Time: %s", threads, nChunks, timeStr));
        }

        public static String getAndReset(){
            StringBuilder out = new StringBuilder();
            for (String s:l)
                out.append(s).append('\n');

            l = new LinkedList<>();

            return out.toString();
        }
    }

    /**
     * Print the heap structure
     */
    public boolean analyze(){
        return memory.analyzer.analyze(false, false);
    }

    /**
     * Check the heap for errors
     *
     * @param p_dumpOnError Dump the heap on a error
     * @return True if no error was found, else false
     */
    @SuppressWarnings("UnusedReturnValue")
    public boolean checkForError(final boolean p_dumpOnError){
        return memory.analyzer.analyze(true, p_dumpOnError);
    }

}
