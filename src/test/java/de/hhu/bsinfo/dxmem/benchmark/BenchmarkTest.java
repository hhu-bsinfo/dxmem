package de.hhu.bsinfo.dxmem.benchmark;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.DXMemory;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.benchmark.operation.Create;
import de.hhu.bsinfo.dxmem.benchmark.operation.Get;
import de.hhu.bsinfo.dxmem.data.ChunkID;

public class BenchmarkTest {
    @Test
    public void create() {
        Configurator.setRootLevel(Level.DEBUG);
        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);

        Benchmark benchmark = new Benchmark("test");
        benchmark.addPhase(new BenchmarkPhase("test", memory, 1, 1000000, 0, new Create(1.0f, 1, 32, 32)));

        benchmark.execute();

        memory.shutdown();
    }

    @Test
    public void create2Threads() {
        Configurator.setRootLevel(Level.DEBUG);
        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);

        Benchmark benchmark = new Benchmark("test");
        benchmark.addPhase(new BenchmarkPhase("test", memory, 2, 1000000, 0, new Create(1.0f, 1, 32, 32)));

        benchmark.execute();

        memory.shutdown();
    }

    @Test
    public void get() {
        Configurator.setRootLevel(Level.DEBUG);
        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);

        Benchmark benchmark = new Benchmark("test");
        benchmark.addPhase(new BenchmarkPhase("test", memory, 1, 1000000, 0,
                new Create(0.5f, 1, 32, 32), new Get(0.5f, 1,
                    ChunkID.getChunkID(DXMemoryTestConstants.NODE_ID, 0),
                    ChunkID.getChunkID(DXMemoryTestConstants.NODE_ID, 10), 32)));

        benchmark.execute();

        memory.shutdown();
    }

    @Test
    public void get2Threads() {
        Configurator.setRootLevel(Level.DEBUG);
        DXMemory memory = new DXMemory(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);

        Benchmark benchmark = new Benchmark("test");
        benchmark.addPhase(new BenchmarkPhase("test", memory, 2, 1000, 0,
                new Create(0.5f, 1, 32, 32), new Get(0.5f, 1,
                ChunkID.getChunkID(DXMemoryTestConstants.NODE_ID, 0),
                ChunkID.getChunkID(DXMemoryTestConstants.NODE_ID, 10), 32)));

        benchmark.execute();

        memory.shutdown();
    }
}
