package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterChunkId;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;

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

        ChunkState[] state = new ChunkState[1];

        byte[] data = CliContext.getInstance().getMemory().get().get(state, 0, m_cid);

        if (state[0] != ChunkState.OK) {
            System.out.printf("ERROR: Getting chunk for put %s: %s\n", ChunkID.toHexString(m_cid), state[0]);
            return;
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        byteBuffer.position(m_offset);

        switch (m_type) {
            case "str":
                try {
                    int size = byteBuffer.capacity() - byteBuffer.position();
                    if (data.length < size) {
                        size = data.length;
                    }

                    byteBuffer.put(data, 0, size);
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

        ChunkByteArray chunk = new ChunkByteArray(data);
        chunk.setID(m_cid);

        CliContext.getInstance().getMemory().put().put(chunk);

        if (chunk.isStateOk()) {
            System.out.printf("Putting chunk %s successful\n", ChunkID.toHexString(m_cid));
        } else {
            System.out.printf("ERROR: Putting chunk %s: %s\n", ChunkID.toHexString(m_cid), chunk.getState());
        }
    }
}
