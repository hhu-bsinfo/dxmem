package de.hhu.bsinfo.dxmem.cli;

import picocli.CommandLine;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;

import de.hhu.bsinfo.dxmem.cli.debugger.SubcmdsDebugger;

@CommandLine.Command(
        name = "debugger",
        description = "Interactive debugger session to analyze and debug (dumped) dxmem instances"
)
public class ToolDebugger implements Runnable {
    @Override
    public void run() {
        System.out.println("Running interactive debugger shell");
        System.out.println("Use the 'help' command to get a list of available commands");

        LineReader reader = LineReaderBuilder.builder().build();
        CommandLine cmd = new CommandLine(new SubcmdsDebugger());

        while (true) {
            String prompt;
            String line;

            if (CliContext.getInstance().isMemoryLoaded()) {
                prompt = "LOADED > ";
            } else {
                prompt = "EMPTY > ";
            }

            try {
                line = reader.readLine(prompt);

                if (line.isEmpty()) {
                    continue;
                }

                String[] args = line.split(" ");

                cmd.parseWithHandler(new CommandLine.RunAll(), System.err, args);
            } catch (UserInterruptException ignored) {
                // Ignore
            } catch (EndOfFileException ignored) {
                System.exit(-1);
            }
        }
    }
}
