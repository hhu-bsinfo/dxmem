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

import de.hhu.bsinfo.dxmem.cli.CliContext;

/**
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
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

    @CommandLine.Parameters(
            index = "1",
            arity = "0..1",
            paramLabel = "disableChunkLocks",
            description = "Disable the chunk locks (see DXMem class documentation for details)")
    private boolean m_disableChunkLocks = false;

    @Override
    public void run() {
        CliContext.getInstance().loadFromFile(m_inputFile, m_disableChunkLocks);
    }
}
