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

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterChunkId;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;

/**
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
@CommandLine.Command(
        name = "put",
        description = "Put data to a chunk"
)
public class CmdPut implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            converter = TypeConverterChunkId.class,
            paramLabel = "cid",
            description = "CID of the chunk to put. nid/lid also allowed, e.g. 0x1234/0x10")
    private long m_cid;

    @CommandLine.Parameters(
            index = "1",
            paramLabel = "offset",
            description = "Offset within the chunk to start putting data to")
    private int m_offset = 0;

    @CommandLine.Parameters(
            index = "2",
            paramLabel = "type",
            description = "Format of data to put (str, byte, short, int, long)")
    private String m_type = "byte";

    @CommandLine.Parameters(
            index = "3",
            paramLabel = "data",
            description = "Data to put")
    private String m_data;

    @Override
    public void run() {
        if (!CliContext.getInstance().isMemoryLoaded()) {
            System.out.println("ERROR: No memory instance loaded");
            return;
        }

        ChunkByteArray chunk = CliContext.getInstance().getMemory().get().get(m_cid);

        if (!chunk.isStateOk()) {
            System.out.printf("ERROR: Getting chunk for put %s: %s\n", ChunkID.toHexString(m_cid), chunk.getState());
            return;
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(chunk.getData());
        byteBuffer.position(m_offset);

        switch (m_type) {
            case "str":
                try {
                    int size = byteBuffer.capacity() - byteBuffer.position();
                    if (chunk.getSize() < size) {
                        size = chunk.getSize();
                    }

                    byteBuffer.put(chunk.getData(), 0, size);
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "byte":
                try {
                    byteBuffer.put((byte) (Integer.parseInt(m_data) & 0xFF));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "short":
                try {
                    byteBuffer.putShort((short) (Integer.parseInt(m_data) & 0xFFFF));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "int":
                try {
                    byteBuffer.putInt(Integer.parseInt(m_data));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "long":
                try {
                    byteBuffer.putLong(Long.parseLong(m_data));
                } catch (final Exception ignored) {
                    // that's fine, trunc data
                }

                break;

            case "hex":
                String[] tokens = m_data.split(" ");

                for (String str : tokens) {
                    try {
                        byteBuffer.put((byte) Integer.parseInt(str, 16));
                    } catch (final Exception ignored) {
                        // that's fine, trunc data
                    }
                }

                break;

            default:
                System.out.printf("Unsupported data type %s\n", m_type);
                return;
        }

        CliContext.getInstance().getMemory().put().put(chunk);

        if (chunk.isStateOk()) {
            System.out.printf("Putting chunk %s successful\n", ChunkID.toHexString(m_cid));
        } else {
            System.out.printf("ERROR: Putting chunk %s: %s\n", ChunkID.toHexString(m_cid), chunk.getState());
        }
    }
}
