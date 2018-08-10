package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

@CommandLine.Command(
        name = "logger",
        description = "Set the log level of the logger"
)
public class CmdLogger implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            paramLabel = "logLevel",
            description = "Log level for the logger to set (trace, debug, info, warn, error, off)")
    private String m_inputFile;

    @Override
    public void run() {
        Configurator.setRootLevel(Level.toLevel(m_inputFile));
    }
}
