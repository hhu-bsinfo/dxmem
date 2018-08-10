package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.benchmark.Benchmark;
import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.cli.ToolBenchmark;

public abstract class AbstractWorkload implements Runnable {
    @CommandLine.ParentCommand
    private Object m_parent;

    public abstract Benchmark createWorkload();

    @Override
    public void run() {
        if (m_parent instanceof ToolBenchmark) {
            ToolBenchmark parent = (ToolBenchmark) m_parent;

            // for the standalone tool, we have to create a new heap
            parent.init();
        } else {
            if (!CliContext.getInstance().isMemoryLoaded()) {
                System.out.println("ERROR: No memory instance loaded");
                return;
            }
        }

        createWorkload().execute();
    }
}
