package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookA;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookB;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookC;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookD;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookE;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookF;
import de.hhu.bsinfo.dxmem.benchmark.workload.MemVar;
import de.hhu.bsinfo.dxmem.benchmark.workload.YcsbA;
import de.hhu.bsinfo.dxmem.benchmark.workload.YcsbB;
import de.hhu.bsinfo.dxmem.benchmark.workload.YcsbC;

@CommandLine.Command(
        name = "benchmark",
        description = "Run a benchmark on the current instance of DXMem",
        customSynopsis = "@|bold benchmark|@ @|yellow WORKLOAD|@ [...]",
        subcommands = {
                FacebookA.class,
                FacebookB.class,
                FacebookC.class,
                FacebookD.class,
                FacebookE.class,
                FacebookF.class,
                MemVar.class,
                YcsbA.class,
                YcsbB.class,
                YcsbC.class,
        }
)
public class CmdBenchmark implements Runnable {
    @Override
    public void run() {
        if (!CliContext.getInstance().isMemoryLoaded()) {
            System.out.println("ERROR: No memory instance loaded");
            return;
        }

        // gets executed if no benchmark was selected
        CommandLine.usage(this, System.out);
    }
}
