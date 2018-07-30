package de.hhu.bsinfo.dxmem.benchmark.workload;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.benchmark.Benchmark;
import de.hhu.bsinfo.dxmem.benchmark.BenchmarkPhase;
import de.hhu.bsinfo.dxmem.benchmark.operation.Create;
import de.hhu.bsinfo.dxmem.benchmark.operation.Get;
import de.hhu.bsinfo.dxmem.benchmark.operation.Put;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

public class FacebookE implements Workload {
    private static final int OBJECT_SIZE = 64;
    private static final int BATCH_COUNT = 1;

    @Override
    public String getName() {
        return "facebook-e";
    }

    @Override
    public String getDescription() {
        return "Facebook graph workload with 1x 64 byte objects (50% get, 50% put)";
    }

    @Override
    public Benchmark createWorkload(final String[] p_args) {
        if (p_args.length < 5) {
            System.out.println("Not sufficient parameters for workload facebook-e");
            System.out.println("Args: <heap size with postfix, e.g. 128-mb, 1-gb> <num load threads> " +
                    "<load total objects> <num run threads> <run total operations>");
            return null;
        }

        String[] splitSize = p_args[0].split("-");
        StorageUnit heapSize = new StorageUnit(Long.parseLong(splitSize[0]), splitSize[1]);
        int loadThreads = Integer.parseInt(p_args[1]);
        long loadTotalObjects = Long.parseLong(p_args[2]);
        int runThreads = Integer.parseInt(p_args[3]);
        long runTotalOperations = Long.parseLong(p_args[4]);

        return create(heapSize, loadThreads, loadTotalObjects, runThreads, runTotalOperations);
    }

    private Benchmark create(final StorageUnit p_heapSize, final int p_loadThreads,
            final long p_loadTotalObjects, final int p_runThreads, final long p_runTotalOperations) {
        DXMem memory = new DXMem((short) 0, p_heapSize.getBytes());

        Benchmark benchmark = new Benchmark(getName());

        benchmark.addPhase(new BenchmarkPhase("load", memory, p_loadThreads, p_loadTotalObjects, 0,
                new Create(1.0f, BATCH_COUNT, OBJECT_SIZE, OBJECT_SIZE)));
        benchmark.addPhase(new BenchmarkPhase("run", memory, p_runThreads, p_runTotalOperations, 0,
                new Get(0.5f, BATCH_COUNT, 0, p_loadTotalObjects - 1, OBJECT_SIZE),
                new Put(0.5f, BATCH_COUNT, 0, p_loadTotalObjects - 1, OBJECT_SIZE)));

        return benchmark;
    }
}
