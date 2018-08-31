/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

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
