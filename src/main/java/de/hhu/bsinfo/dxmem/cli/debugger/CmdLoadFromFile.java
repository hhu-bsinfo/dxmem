package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.cli.CliContext;

@CommandLine.Command(
        name = "load",
        description = "Load a stored DXMemory dump from a file"
)
public class CmdLoadFromFile implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            paramLabel = "inputFile",
            description = "Path to input file which contains a DXMemory dump")
    private String m_inputFile;

    @Override
    public void run() {
        CliContext.getInstance().loadFromFile(m_inputFile);
    }
}
