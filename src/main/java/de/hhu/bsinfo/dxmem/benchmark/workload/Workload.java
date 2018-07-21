package de.hhu.bsinfo.dxmem.benchmark.workload;

import de.hhu.bsinfo.dxmem.benchmark.Benchmark;

public interface Workload {
    String getName();

    String getDescription();

    Benchmark createWorkload(final String[] p_args);
}
