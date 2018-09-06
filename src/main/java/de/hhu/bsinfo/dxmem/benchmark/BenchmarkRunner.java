package de.hhu.bsinfo.dxmem.benchmark;

/**
 * Interface for a benchmark runner implementation
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 06.09.2018
 */
public interface BenchmarkRunner {
    /**
     * Run the benchmark
     *
     * @param p_benchmark
     *         Benchmark with phases to run
     */
    void runBenchmark(final Benchmark p_benchmark);
}
