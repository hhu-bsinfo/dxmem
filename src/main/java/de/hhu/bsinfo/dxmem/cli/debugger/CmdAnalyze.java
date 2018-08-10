package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.cli.CliContext;

@CommandLine.Command(
        name = "analyze",
        description = "Analyze the currently loaded heap instance"
)
public class CmdAnalyze implements Runnable {
    @Override
    public void run() {
        if (!CliContext.getInstance().isMemoryLoaded()) {
            System.out.println("ERROR: No memory instance loaded");
            return;
        }

        if (CliContext.getInstance().getMemory().analyze().analyze()) {
            System.out.println("Analyzing heap successful, no errors");
        } else {
            System.out.println("Analyzing heap successful, errors detected (see log output)");
        }
    }
}