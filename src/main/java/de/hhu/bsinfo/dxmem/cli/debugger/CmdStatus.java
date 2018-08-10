package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.cli.CliContext;

@CommandLine.Command(
        name = "status",
        description = "Print some status information about the current DXMemory instance"
)
public class CmdStatus implements Runnable {
    @Override
    public void run() {
        if (!CliContext.getInstance().isMemoryLoaded()) {
            System.out.println("ERROR: No memory instance loaded");
            return;
        }

        System.out.println("============= Heap =============");
        System.out.println(CliContext.getInstance().getMemory().stats().getHeapStatus());
        System.out.println("============= CIDTable =============");
        System.out.println(CliContext.getInstance().getMemory().stats().getCIDTableStatus());
        System.out.println("============= LIDStore =============");
        System.out.println(CliContext.getInstance().getMemory().stats().getLIDStoreStatus());
    }
}
