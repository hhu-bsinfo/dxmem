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
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterNodeId;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxutils.NodeID;

@CommandLine.Command(
        name = "ls",
        description = "List all active chunk IDs"
)
public class CmdList implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            arity = "0..1",
            converter = TypeConverterNodeId.class,
            paramLabel = "nodeId",
            description = "Limit listing to chunks of specified node ID")
    private short m_nodeId = NodeID.INVALID_ID;

    @Override
    public void run() {
        if (!CliContext.getInstance().isMemoryLoaded()) {
            System.out.println("ERROR: No memory instance loaded");
            return;
        }

        ChunkIDRanges ranges;

        if (m_nodeId == NodeID.INVALID_ID) {
            ranges = CliContext.getInstance().getMemory().cidStatus().getCIDRangesOfChunks();
        } else {
            ranges = CliContext.getInstance().getMemory().cidStatus().getCIDRangesOfChunks(m_nodeId);
        }

        System.out.printf("CIDRanges of %s: %s\n", m_nodeId != NodeID.INVALID_ID ? NodeID.toHexString(m_nodeId) : "all",
                ranges);
    }
}
