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
