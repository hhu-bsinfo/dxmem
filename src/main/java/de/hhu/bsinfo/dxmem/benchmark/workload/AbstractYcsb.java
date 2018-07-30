package de.hhu.bsinfo.dxmem.benchmark.workload;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.benchmark.Benchmark;
import de.hhu.bsinfo.dxmem.benchmark.BenchmarkPhase;
import de.hhu.bsinfo.dxmem.benchmark.operation.Create;
import de.hhu.bsinfo.dxmem.benchmark.operation.Get;
import de.hhu.bsinfo.dxmem.benchmark.operation.Put;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

public abstract class AbstractYcsb implements Workload {
    private final int m_batchCount;
    private final int m_objectSize;
    private final float m_probGet;
    private final float m_probPut;

    AbstractYcsb(final int p_batchCount, final int p_objectSize, final float p_probGet,
            final float p_probPut) {
        m_batchCount = p_batchCount;
        m_objectSize = p_objectSize;
        m_probGet = p_probGet;
        m_probPut = p_probPut;
    }

    @Override
    public Benchmark createWorkload(final String[] p_args) {
        if (p_args.length < 6) {
            System.out.println("Not sufficient parameters for workload" + getName());
            System.out.println("Args: <heap size with postfix, e.g. 128-mb, 1-gb> <verify data (0/1)>" +
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
                new Create(1.0f, m_batchCount, p_verifyData, m_objectSize, m_objectSize)));
        benchmark.addPhase(new BenchmarkPhase("run", memory, p_runThreads, p_runTotalOperations, 0,
                new Get(m_probGet, m_batchCount, p_verifyData, 0, p_loadTotalObjects - 1, m_objectSize),
                new Put(m_probPut, m_batchCount, p_verifyData, 0, p_loadTotalObjects - 1, m_objectSize)));

        return benchmark;
    }
}
