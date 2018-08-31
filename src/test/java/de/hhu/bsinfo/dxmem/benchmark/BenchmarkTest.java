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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.DXMemoryTestConstants;
import de.hhu.bsinfo.dxmem.benchmark.operation.Create;
import de.hhu.bsinfo.dxmem.benchmark.operation.Get;

public class BenchmarkTest {
    @Test
    public void create() {
        Configurator.setRootLevel(Level.DEBUG);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);

        Benchmark benchmark = new Benchmark("test");
        benchmark.addPhase(new BenchmarkPhase("test", memory, 1, 1000000, 0, new Create(1.0f, 1, true, 32, 32)));

        benchmark.execute();

        memory.shutdown();
    }

    @Test
    public void create2Threads() {
        Configurator.setRootLevel(Level.DEBUG);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);

        Benchmark benchmark = new Benchmark("test");
        benchmark.addPhase(new BenchmarkPhase("test", memory, 2, 1000000, 0, new Create(1.0f, 1, true, 32, 32)));

        benchmark.execute();

        memory.shutdown();
    }

    @Test
    public void get() {
        Configurator.setRootLevel(Level.DEBUG);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);

        Benchmark benchmark = new Benchmark("test");
        benchmark.addPhase(new BenchmarkPhase("test", memory, 1, 1000000, 0,
                new Create(0.5f, 1, true, 32, 32), new Get(0.5f, 1, true, 32)));

        benchmark.execute();

        memory.shutdown();
    }

    @Test
    public void get2Threads() {
        Configurator.setRootLevel(Level.DEBUG);
        DXMem memory = new DXMem(DXMemoryTestConstants.NODE_ID, DXMemoryTestConstants.HEAP_SIZE_MEDIUM);

        Benchmark benchmark = new Benchmark("test");
        benchmark.addPhase(new BenchmarkPhase("test", memory, 2, 1000, 0,
                new Create(0.5f, 1, true, 32, 32), new Get(0.5f, 1, true, 32)));

        benchmark.execute();

        memory.shutdown();
    }
}
