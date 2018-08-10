package de.hhu.bsinfo.dxmem.cli;

import picocli.CommandLine;

@CommandLine.Command(
        name = "dxmem",
        description = "DXMemory command line tool with various subtools for debugging and development",
        subcommands = {
                ToolAnalyzer.class,
                ToolBenchmark.class,
                ToolDebugger.class
        }
)
public class ToolRoot implements Runnable {
    @Override
    public void run() {
        // gets executed if no subtool was selected
        CommandLine.usage(this, System.out);
    }
}
