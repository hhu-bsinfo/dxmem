package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

@CommandLine.Command(
        name = "ycsb-c",
        description = "YCSB workload with 10x 100 byte objects (1.0 get)"
)
public class YcsbC extends AbstractYcsb {
    public YcsbC() {
        super("ycsb-c", 10, 100, 1.0f, 0.0f);
    }
}
