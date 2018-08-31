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

import java.util.ArrayList;
import java.util.List;
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
