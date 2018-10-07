package de.hhu.bsinfo.dxmem.cli;

import picocli.CommandLine;

import de.hhu.bsinfo.dxmem.core.MemoryOverheadCalculator;

/**
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 07.10.2018
 */
@CommandLine.Command(
        name = "overhead",
        description = "Calculate the required space and overhead of the memory manager for a specific data set"
)
public class ToolOverheadCalc implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            paramLabel = "chunkPayloadSize",
            description = "Size of the payload to store in a single chunk")
    private int m_chunkPayloadSize;

    @CommandLine.Parameters(
            index = "1",
            paramLabel = "totalChunkCount",
            description = "Total number of chunks to store using the memory manager")
    private long m_totalChunkCount;

    @Override
    public void run() {
        MemoryOverheadCalculator calc = new MemoryOverheadCalculator(m_chunkPayloadSize, m_totalChunkCount);
        System.out.println(calc);
    }
}
