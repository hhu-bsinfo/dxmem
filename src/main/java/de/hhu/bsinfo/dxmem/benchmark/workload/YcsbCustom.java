package de.hhu.bsinfo.dxmem.benchmark.workload;

import de.hhu.bsinfo.dxmem.benchmark.Benchmark;
import de.hhu.bsinfo.dxmem.benchmark.BenchmarkPhase;
import de.hhu.bsinfo.dxmem.benchmark.operation.Create;
import de.hhu.bsinfo.dxmem.benchmark.operation.Dump;
import de.hhu.bsinfo.dxmem.benchmark.operation.Get;
import de.hhu.bsinfo.dxmem.benchmark.operation.Put;
import picocli.CommandLine;

@CommandLine.Command(
        name = "ycsb-custom",
        description = "Customizable YCSB workload"
)
public class YcsbCustom extends AbstractWorkload {

    @CommandLine.Parameters(
            index = "0",
            paramLabel = "<verifyData>",
            description = "Verify the contents of the chunks after a get operation")
    private boolean m_verifyData;

    @CommandLine.Parameters(
            index = "1",
            paramLabel = "<dumpMemory>",
            description = "Create a memory dump after the benchmark finished")
    private boolean m_dumpMemory;

    @CommandLine.Parameters(
            index = "2",
            paramLabel = "<loadThreads>",
            description = "Number of threads to use for loading phase")
    private int m_loadThreads;

    @CommandLine.Parameters(
            index = "3",
            paramLabel = "<loadTotalObjects>",
            description = "Total number of objects to load (on all threads)")
    private long m_loadTotalObjects;

    @CommandLine.Parameters(
            index = "4",
            paramLabel = "<runThreads>",
            description = "Number of threads to use during run phase")
    private int m_runThreads;

    @CommandLine.Parameters(
            index = "5",
            paramLabel = "<runTotalOperations>",
            description = "Total number of run operations to execute (on all threads)")
    private long m_runTotalOperations;

    @CommandLine.Parameters(
            index = "6",
            paramLabel = "<batchCount>",
            description = "Batch count for operation"
    )
    private int m_batchCount;

    @CommandLine.Parameters(
            index = "7",
            paramLabel = "<objectSize>",
            description = "Size of a single object"
    )
    private int m_objectSize;

    @CommandLine.Parameters(
            index = "8",
            paramLabel = "<probGet>",
            description = "Probability for get operations"
    )
    private float m_probGet;

    @CommandLine.Parameters(
            index = "9",
            paramLabel = "<probPut>",
            description = "Probability for put operations"
    )
    private float m_probPut;

    @Override
    public Benchmark createWorkload() {
        Benchmark benchmark = new Benchmark("ycsb-custom");

        benchmark.addPhase(new BenchmarkPhase("load", m_loadThreads, m_loadTotalObjects, 0,
                new Create(1.0f, m_batchCount, m_verifyData, m_objectSize, m_objectSize)));
        benchmark.addPhase(new BenchmarkPhase("run", m_runThreads, m_runTotalOperations, 0,
                new Get(m_probGet, m_batchCount, m_verifyData, m_objectSize),
                new Put(m_probPut, m_batchCount, m_verifyData, m_objectSize)));

        if (m_dumpMemory) {
            benchmark.addPhase(new BenchmarkPhase("dump", 1, 1, 0, new Dump("benchmark_mem_var.dump")));
        }

        return benchmark;
    }
}
