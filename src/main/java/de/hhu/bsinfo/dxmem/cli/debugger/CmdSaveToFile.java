package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.cli.CliContext;

@CommandLine.Command(
        name = "save",
        description = "Save the currently loaded DXMemory to a file"
)
public class CmdSaveToFile implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            paramLabel = "outputFile",
            description = "Path to output file to save the current DXMemory instance to")
    private String m_outputFile;

    @Override
    public void run() {
        if (!CliContext.getInstance().isMemoryLoaded()) {
            System.out.println("ERROR: No memory instance loaded");
            return;
        }

        CliContext.getInstance().getMemory().dump().dump(m_outputFile);
    }
}
