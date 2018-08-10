package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.benchmark.Benchmark;
import de.hhu.bsinfo.dxmem.benchmark.BenchmarkPhase;
import de.hhu.bsinfo.dxmem.benchmark.operation.Create;
import de.hhu.bsinfo.dxmem.benchmark.operation.Dump;
import de.hhu.bsinfo.dxmem.benchmark.operation.Get;
import de.hhu.bsinfo.dxmem.benchmark.operation.Put;
import de.hhu.bsinfo.dxmem.benchmark.operation.Remove;
import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterStorageUnit;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

@CommandLine.Command(
        name = "mem-var",
        description = "Generic and variable memory workload. Define benchmark parameters via arguments"
)
public class MemVar extends AbstractWorkload {
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
            converter = TypeConverterStorageUnit.class,
            paramLabel = "<objectSizeMin>",
            description = "Minimum object size in bytes or StorageUnit")
    private StorageUnit m_objectSizeMin;

    @CommandLine.Parameters(
            index = "3",
            converter = TypeConverterStorageUnit.class,
            paramLabel = "<objectSizeMax>",
            description = "Maximum object size in bytes or StorageUnit")
    private StorageUnit m_objectSizeMax;

    @CommandLine.Parameters(
            index = "4",
            paramLabel = "<batchCount>",
            description = "Batch size for a single operation")
    private int m_batchCount;

    @CommandLine.Parameters(
            index = "5",
            paramLabel = "<loadThreads>",
            description = "Number of threads to use for loading phase")
    private int m_loadThreads;

    @CommandLine.Parameters(
            index = "6",
            paramLabel = "<loadTotalObjects>",
            description = "Total number of objects to load (on all threads)")
    private long m_loadTotalObjects;

    @CommandLine.Parameters(
            index = "7",
            paramLabel = "<runThreads>",
            description = "Number of threads to use during run phase")
    private int m_runThreads;

    @CommandLine.Parameters(
            index = "8",
            paramLabel = "<runTotalOperations>",
            description = "Total number of run operations to execute (on all threads)")
    private long m_runTotalOperations;

    @CommandLine.Parameters(
            index = "9",
            paramLabel = "<probCreate>",
            description = "Create operation probability")
    private float m_probCreate;

    @CommandLine.Parameters(
            index = "10",
            paramLabel = "<probGet>",
            description = "Get operation probability")
    private float m_probGet;

    @CommandLine.Parameters(
            index = "11",
            paramLabel = "<probPut>",
            description = "Put operation probability")
    private float m_probPut;

    @CommandLine.Parameters(
            index = "12",
            paramLabel = "<probRemove>",
            description = "Remove operation probability")
    private float m_probRemove;

    public MemVar() {

    }

    public MemVar(final boolean p_verifyData, final boolean p_dumpMemory, final StorageUnit p_objectSizeMin,
            final StorageUnit p_objectSizeMax, final int p_batchCount, final int p_loadThreads,
            final long p_totalLoadObjects, final int p_runThreads, final long p_runTotalOperations,
            final float p_probCreate, final float p_probGet, final float p_probPut, final float p_probRemove) {
        m_verifyData = p_verifyData;
        m_dumpMemory = p_dumpMemory;
        m_objectSizeMin = p_objectSizeMin;
        m_objectSizeMax = p_objectSizeMax;
        m_batchCount = p_batchCount;
        m_loadThreads = p_loadThreads;
        m_loadTotalObjects = p_totalLoadObjects;
        m_runThreads = p_runThreads;
        m_runTotalOperations = p_runTotalOperations;
        m_probCreate = p_probCreate;
        m_probGet = p_probGet;
        m_probPut = p_probPut;
        m_probRemove = p_probRemove;
    }

    @Override
    public Benchmark createWorkload() {
        Benchmark benchmark = new Benchmark("mem-var");

        benchmark.addPhase(new BenchmarkPhase("load", CliContext.getInstance().getMemory(), m_loadThreads,
                m_loadTotalObjects, 0,
                new Create(1.0f, m_batchCount, m_verifyData, (int) m_objectSizeMin.getBytes(),
                        (int) m_objectSizeMax.getBytes())));

        benchmark.addPhase(new BenchmarkPhase("run", CliContext.getInstance().getMemory(), m_runThreads,
                m_runTotalOperations, 0,
                new Get(m_probGet, m_batchCount, m_verifyData, (int) m_objectSizeMax.getBytes()),
                new Put(m_probPut, m_batchCount, m_verifyData, (int) m_objectSizeMax.getBytes()),
                new Create(m_probCreate, m_batchCount, m_verifyData, (int) m_objectSizeMin.getBytes(),
                        (int) m_objectSizeMax.getBytes()),
                new Remove(m_probRemove, m_batchCount, m_verifyData)));

        if (m_dumpMemory) {
            benchmark.addPhase(new BenchmarkPhase("dump", CliContext.getInstance().getMemory(), 1, 1, 0,
                    new Dump("benchmark_mem_var.dump")));
        }

        return benchmark;
    }
}
