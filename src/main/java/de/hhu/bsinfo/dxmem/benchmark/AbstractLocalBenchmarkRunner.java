/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxmem.benchmark;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;

/**
 * Benchmark runner for local instance only
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public abstract class AbstractLocalBenchmarkRunner implements BenchmarkRunner {
    private final BenchmarkContext m_context;

    /**
     * Constructor
     */
    protected AbstractLocalBenchmarkRunner(final BenchmarkContext p_context) {
        m_context = p_context;
    }

    /**
     * Allow sub-class to initialize things like setting up the heap before running the benchmark
     *
     * @return True if init successful, false on error (benchmark aborts)
     */
    public abstract boolean init();

    @Override
    public void runBenchmark(final Benchmark p_benchmark) {
        if (!init()) {
            return;
        }

        System.out.println("Executing benchmark '" + p_benchmark.getName() + '\'');

        ChunkIDRanges cidRanges = new ChunkIDRanges();
        ReentrantReadWriteLock cidRangesLock = new ReentrantReadWriteLock(false);

        for (BenchmarkPhase phase : p_benchmark.getPhases()) {
            System.out.println("Executing benchmark phase '" + phase.getName() + "'...");
            phase.execute(m_context, cidRanges, cidRangesLock);
            System.out.println("Results of benchmark phase '" + phase.getName() + "'...");
            phase.printResults(m_context);
        }

        System.out.println("Finished executing benchmark '" + p_benchmark.getName() + '\'');
    }
}
