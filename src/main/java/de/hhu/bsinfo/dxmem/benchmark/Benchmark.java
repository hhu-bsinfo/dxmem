package de.hhu.bsinfo.dxmem.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;

public class Benchmark {
    private final String m_name;
    private final List<BenchmarkPhase> m_phases;

    public Benchmark(final String p_name) {
        m_name = p_name;
        m_phases = new ArrayList<>();
    }

    public void addPhase(final BenchmarkPhase p_phase) {
        m_phases.add(p_phase);
    }

    public void execute() {
        System.out.println("Executing benchmark '" + m_name + "'");

        ChunkIDRanges cidRanges = new ChunkIDRanges();
        ReentrantReadWriteLock cidRangesLock = new ReentrantReadWriteLock(false);

        for (BenchmarkPhase phase : m_phases) {
            System.out.println("Executing benchmark phase '" + phase.getName() + "'...");
            phase.execute(cidRanges, cidRangesLock);
            System.out.println("Results of benchmark phase '" + phase.getName() + "'...");
            phase.printResults();
        }

        System.out.println("Finished executing benchmark '" + m_name + "'");
    }
}
