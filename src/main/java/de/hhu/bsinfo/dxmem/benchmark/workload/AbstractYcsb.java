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

package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.benchmark.Benchmark;
import de.hhu.bsinfo.dxmem.benchmark.BenchmarkPhase;
import de.hhu.bsinfo.dxmem.benchmark.operation.Create;
import de.hhu.bsinfo.dxmem.benchmark.operation.Dump;
import de.hhu.bsinfo.dxmem.benchmark.operation.Get;
import de.hhu.bsinfo.dxmem.benchmark.operation.Put;
import de.hhu.bsinfo.dxmem.cli.CliContext;

/**
 * Base class for YCSB type benchmarks
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public abstract class AbstractYcsb extends AbstractWorkload {
    private final String m_name;
    private final int m_batchCount;
    private final int m_objectSize;
    private final float m_probGet;
    private final float m_probPut;

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

    /**
     * Constructor
     *
     * @param p_name
     *         Name of the benchmark
     * @param p_batchCount
     *         Batch count for operation
     * @param p_objectSize
     *         Size of a single object
     * @param p_probGet
     *         Probability for get operations
     * @param p_probPut
     *         Probability for put operations
     */
    AbstractYcsb(final String p_name, final int p_batchCount, final int p_objectSize, final float p_probGet,
            final float p_probPut) {
        m_name = p_name;
        m_batchCount = p_batchCount;
        m_objectSize = p_objectSize;
        m_probGet = p_probGet;
        m_probPut = p_probPut;
    }

    @Override
    public Benchmark createWorkload() {
        Benchmark benchmark = new Benchmark(m_name);

        benchmark.addPhase(new BenchmarkPhase("load", CliContext.getInstance().getMemory(), m_loadThreads,
                m_loadTotalObjects, 0,
                new Create(1.0f, m_batchCount, m_verifyData, m_objectSize, m_objectSize)));
        benchmark.addPhase(new BenchmarkPhase("run", CliContext.getInstance().getMemory(), m_runThreads,
                m_runTotalOperations, 0,
                new Get(m_probGet, m_batchCount, m_verifyData, m_objectSize),
                new Put(m_probPut, m_batchCount, m_verifyData, m_objectSize)));

        if (m_dumpMemory) {
            benchmark.addPhase(new BenchmarkPhase("dump", CliContext.getInstance().getMemory(), 1, 1, 0,
                    new Dump("benchmark_mem_var.dump")));
        }

        return benchmark;
    }
}
