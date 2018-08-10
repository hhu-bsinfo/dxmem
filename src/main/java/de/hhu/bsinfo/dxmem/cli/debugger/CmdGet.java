package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterChunkId;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;

@CommandLine.Command(
        name = "get",
        description = "Get the contents of an existing chunk"
)
public class CmdGet implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            converter = TypeConverterChunkId.class,
            paramLabel = "cid",
            description = "CID of the chunk to get. nid/lid also allowed, e.g. 0x1234/0x10")
    private long m_cid;

    @CommandLine.Parameters(
            index = "1",
            arity = "0..1",
            paramLabel = "type",
            description = "Format to print the data (str, byte, short, int, long; defaults to byte)")
    private String m_type = "byte";

    @CommandLine.Parameters(
            index = "2",
            arity = "0..1",
            paramLabel = "hex",
            description = "For some representations, print as hex instead of decimal, defaults to true")
    private boolean m_hex = true;

    @CommandLine.Parameters(
            index = "3",
            arity = "0..1",
            paramLabel = "offset",
            description = "Offset within the chunk to start getting data from, defaults to 0")
    private int m_offset = 0;

    @CommandLine.Parameters(
            index = "4",
            arity = "0..1",
            paramLabel = "length",
            description = "Number of bytes of the chunk to print, defaults to size of chunk")
    private int m_length = -1;

    @Override
    public void run() {
        if (!CliContext.getInstance().isMemoryLoaded()) {
            System.out.println("ERROR: No memory instance loaded");
            return;
        }

        ChunkByteArray chunk = CliContext.getInstance().getMemory().get().get(m_cid);

        if (!chunk.isStateOk()) {
            System.out.printf("ERROR: Getting chunk %s: %s\n", ChunkID.toHexString(m_cid), chunk.getState());
            return;
        }

        if (m_length == -1) {
            m_length = chunk.getSize();
        }

        StringBuilder builder = new StringBuilder();
        ByteBuffer byteBuffer = ByteBuffer.wrap(chunk.getData());
        byteBuffer.position(m_offset);

        switch (m_type) {
            case "str":
                try {
                    builder = new StringBuilder(new String(chunk.getData(), m_offset, m_length, "US-ASCII"));
                } catch (final UnsupportedEncodingException e) {
                    System.out.printf("Error encoding string: %s\n", e.getMessage());
                    return;
                }

                break;

            case "byte":
                for (int i = 0; i < m_length; i += Byte.BYTES) {
                    if (m_hex) {
                        builder.append(Integer.toHexString(byteBuffer.get() & 0xFF)).append(' ');
                    } else {
                        builder.append(byteBuffer.get()).append(' ');
                    }
                }
                break;

            case "short":
                for (int i = 0; i < m_length; i += Short.BYTES) {
                    if (m_hex) {
                        builder.append(Integer.toHexString(byteBuffer.getShort() & 0xFFFF)).append(' ');
                    } else {
                        builder.append(byteBuffer.getShort()).append(' ');
                    }
                }
                break;

            case "int":
                for (int i = 0; i < m_length; i += Integer.BYTES) {
                    if (m_hex) {
                        builder.append(Integer.toHexString(byteBuffer.getInt())).append(' ');
                    } else {
                        builder.append(byteBuffer.getInt()).append(' ');
                    }
                }
                break;

            case "long":
                for (int i = 0; i < m_length; i += Long.BYTES) {
                    if (m_hex) {
                        builder.append(Long.toHexString(byteBuffer.getLong())).append(' ');
                    } else {
                        builder.append(byteBuffer.getLong()).append(' ');
                    }
                }
                break;

            default:
                System.out.printf("Unsuported data type %s\n", m_type);
                return;
        }

        System.out.printf("Chunk data of %s (size %d): \n%s\n", ChunkID.toHexString(m_cid),
                chunk.getSize(), builder.toString());
    }
}
