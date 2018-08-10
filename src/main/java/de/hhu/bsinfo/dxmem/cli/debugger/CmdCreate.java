package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterStorageUnit;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

@CommandLine.Command(
        name = "create",
        description = "Create a new chunk"
)
public class CmdCreate implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            converter = TypeConverterStorageUnit.class,
            paramLabel = "size",
            description = "Size of the chunk in bytes or StorageUnit")
    private StorageUnit m_size;

    @Override
    public void run() {
        if (!CliContext.getInstance().isMemoryLoaded()) {
            System.out.println("ERROR: No memory instance loaded");
            return;
        }

        long cid = CliContext.getInstance().getMemory().create().create((int) m_size.getBytes());

        System.out.println("Chunk created: " + ChunkID.toHexString(cid));
    }
}
