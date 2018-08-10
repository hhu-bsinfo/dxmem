package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

@CommandLine.Command(
        name = "",
        customSynopsis = "[COMMAND] ...",
        subcommands = {
                CmdAnalyze.class,
                CmdBenchmark.class,
                CmdCreate.class,
                CmdExit.class,
                CmdGet.class,
                CmdGet2.class,
                CmdHelp.class,
                CmdList.class,
                CmdLoadFromFile.class,
                CmdLogger.class,
                CmdPut.class,
                CmdRemove.class,
                CmdSaveToFile.class,
                CmdStatus.class,
                CmdNewMemory.class,
        }
)
public class SubcmdsDebugger implements Runnable {
    @Override
    public void run() {

    }
}
