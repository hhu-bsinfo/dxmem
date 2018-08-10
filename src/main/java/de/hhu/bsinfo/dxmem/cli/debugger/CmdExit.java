package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

@CommandLine.Command(
        name = "exit",
        description = "Exit the debugger"
)
public class CmdExit implements Runnable {
    @Override
    public void run() {
        System.exit(0);
    }
}
