package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

@CommandLine.Command(
        name = "help",
        description = "Print the help/usage message"
)
public class CmdHelp implements Runnable {
    @Override
    public void run() {
        // gets executed if no subtool was selected
        CommandLine.usage(SubcmdsDebugger.class, System.out);
    }
}
