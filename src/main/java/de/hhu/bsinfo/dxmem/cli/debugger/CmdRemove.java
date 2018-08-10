package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterChunkId;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;

@CommandLine.Command(
        name = "remove",
        description = "Remove a chunk"
)
public class CmdRemove implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            converter = TypeConverterChunkId.class,
            paramLabel = "cid",
            description = "CID of the chunk to remove. nid/lid also allowed, e.g. 0x1234/0x10")
    private long m_cid;

    @Override
    public void run() {
        if (!CliContext.getInstance().isMemoryLoaded()) {
            System.out.println("ERROR: No memory instance loaded");
            return;
        }

        int size = CliContext.getInstance().getMemory().remove().remove(m_cid);

        if (size > 0) {
            System.out.printf("Chunk %s removed\n", ChunkID.toHexString(m_cid));
        } else {
            System.out.printf("ERROR: Removing chunk %s: %s\n", ChunkID.toHexString(m_cid), ChunkState.values()[-size]);
        }
    }
}
