package de.hhu.bsinfo.dxmem.benchmark.workload;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.benchmark.Benchmark;
import de.hhu.bsinfo.dxmem.benchmark.BenchmarkPhase;
import de.hhu.bsinfo.dxmem.benchmark.operation.Create;
import de.hhu.bsinfo.dxmem.benchmark.operation.Get;
import de.hhu.bsinfo.dxmem.benchmark.operation.Put;
import de.hhu.bsinfo.dxmem.benchmark.operation.Remove;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

public class MemVar implements Workload {
    @Override
    public String getName() {
        return "mem-var";
    }

    @Override
    public String getDescription() {
        return "Generic and variable memory workload. Define benchmark parameters via arguments";
    }

    @Override
    public Benchmark createWorkload(final String[] p_args) {
        if (p_args.length < 12) {
            System.out.println("Not sufficient parameters for workload ycsb-a");
            System.out.println("Args: <heap size with postfix, e.g. 128-mb, 1-gb> <obj size min> <object size max> " +
                    "<batch count> <num load threads> <load total objects> <num run threads> <run total operations> " +
                    "<probability create> <prob. get> <prob. put> <prob. remove>");
            return null;
        }

        String[] splitSize = p_args[0].split("-");
        StorageUnit heapSize = new StorageUnit(Long.parseLong(splitSize[0]), splitSize[1]);
        int objectSizeMin = Integer.parseInt(p_args[1]);
        int objectSizeMax = Integer.parseInt(p_args[2]);
        int batchCount = Integer.parseInt(p_args[3]);
        int loadThreads = Integer.parseInt(p_args[4]);
        long loadTotalObjects = Long.parseLong(p_args[5]);
        int runThreads = Integer.parseInt(p_args[6]);
        long runTotalOperations = Long.parseLong(p_args[7]);
        float probCreate = Float.parseFloat(p_args[8]);
        float probGet = Float.parseFloat(p_args[9]);
        float probPut = Float.parseFloat(p_args[10]);
        float probRemove = Float.parseFloat(p_args[11]);

        return create(heapSize, objectSizeMin, objectSizeMax, batchCount, loadThreads, loadTotalObjects, runThreads,
                runTotalOperations, probCreate, probGet, probPut, probRemove);
    }

    private Benchmark create(final StorageUnit p_heapSize, final int p_objectSizeMin, final int p_objectSizeMax,
            final int p_batchCount, final int p_loadThreads, final long p_loadTotalObjects, final int p_runThreads,
            final long p_runTotalOperations, final float p_probCreate, final float p_probGet, final float p_probPut,
            final float p_probRemove) {
        DXMem memory = new DXMem((short) 0, p_heapSize.getBytes());

        Benchmark benchmark = new Benchmark(getName());

        benchmark.addPhase(new BenchmarkPhase("load", memory, p_loadThreads, p_loadTotalObjects, 0,
                new Create(1.0f, p_batchCount, p_objectSizeMin, p_objectSizeMax)));

        benchmark.addPhase(new BenchmarkPhase("run", memory, p_runThreads, p_runTotalOperations, 0,
                new Get(p_probGet, p_batchCount, 0, p_loadTotalObjects - 1, p_objectSizeMax),
                new Put(p_probPut, p_batchCount, 0, p_loadTotalObjects - 1, p_objectSizeMax),
                new Create(p_probCreate, p_batchCount, p_objectSizeMin, p_objectSizeMax),
                new Remove(p_probRemove, p_batchCount, 0, p_loadTotalObjects - 1)));

        return benchmark;
    }
}
