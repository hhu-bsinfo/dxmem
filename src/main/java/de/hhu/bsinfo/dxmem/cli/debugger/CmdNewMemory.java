package de.hhu.bsinfo.dxmem.cli.debugger;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.cli.CliContext;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterNodeId;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterStorageUnit;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

@CommandLine.Command(
        name = "new",
        description = "Create a new empty dxmemory instance"
)
public class CmdNewMemory implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            converter = TypeConverterNodeId.class,
            paramLabel = "nodeId",
            description = "Node id for local memory management")
    private Short m_nodeId;

    @CommandLine.Parameters(
            index = "1",
            converter = TypeConverterStorageUnit.class,
            paramLabel = "heapSize",
            description = "Total heap size in bytes or StorageUnit")
    private StorageUnit m_heapSize;

    @Override
    public void run() {
        CliContext.getInstance().newMemory(m_nodeId, m_heapSize.getBytes());
    }
}
