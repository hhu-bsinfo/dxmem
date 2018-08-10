package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

@CommandLine.Command(
        name = "ycsb-b",
        description = "YCSB workload with 10x 100 byte objects (0.95 get, 0.05 put)"
)
public class YcsbB extends AbstractYcsb {
    public YcsbB() {
        super("ycsb-b", 10, 100, 0.95f, 0.05f);
    }
}
