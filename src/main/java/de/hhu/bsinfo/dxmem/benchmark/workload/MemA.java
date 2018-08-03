package de.hhu.bsinfo.dxmem.benchmark.workload;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.benchmark.Benchmark;
import de.hhu.bsinfo.dxmem.benchmark.BenchmarkPhase;
import de.hhu.bsinfo.dxmem.benchmark.operation.Create;
import de.hhu.bsinfo.dxmem.benchmark.operation.Get;
import de.hhu.bsinfo.dxmem.benchmark.operation.Put;
import de.hhu.bsinfo.dxmem.benchmark.operation.Remove;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

public class MemA implements Workload {
    @Override
    public String getName() {
        return "mem-a";
    }

    @Override
    public String getDescription() {
        return "Generic memory workload with 1x 1 to 1024 byte objects (40% get, 40% put, 10% create, 10% remove)";
    }

    @Override
    public Benchmark createWorkload(final String[] p_args) {
        if (p_args.length < 6) {
            System.out.println("Not sufficient parameters for workload ycsb-a");
            System.out.println("Args: <heap size with postfix, e.g. 128-mb, 1-gb> <verify data (0/1)> " +
                    "<num load threads> <load total objects> <num run threads> <run total operations>");
            return null;
        }

        String[] splitSize = p_args[0].split("-");
        StorageUnit heapSize = new StorageUnit(Long.parseLong(splitSize[0]), splitSize[1]);
        boolean verifyData = Integer.parseInt(p_args[1]) > 0;
        int loadThreads = Integer.parseInt(p_args[2]);
        long loadTotalObjects = Long.parseLong(p_args[3]);
        int runThreads = Integer.parseInt(p_args[4]);
        long runTotalOperations = Long.parseLong(p_args[5]);

        return create(heapSize, verifyData, loadThreads, loadTotalObjects, runThreads, runTotalOperations);
    }

    private Benchmark create(final StorageUnit p_heapSize, final boolean p_verifyData, final int p_loadThreads,
            final long p_loadTotalObjects, final int p_runThreads, final long p_runTotalOperations) {
        DXMem memory = new DXMem((short) 0, p_heapSize.getBytes());

        Benchmark benchmark = new Benchmark(getName());

        benchmark.addPhase(new BenchmarkPhase("load", memory, p_loadThreads, p_loadTotalObjects, 0,
                new Create(1.0f, 1, p_verifyData, 1, 1024)));
        benchmark.addPhase(new BenchmarkPhase("run", memory, p_runThreads, p_runTotalOperations, 0,
                new Get(0.4f, 1, p_verifyData, 1024),
                new Put(0.4f, 1, p_verifyData, 1024),
                new Create(0.1f, 1, p_verifyData, 1, 1024),
                new Remove(0.1f, 1, p_verifyData)));

        return benchmark;
    }
}
