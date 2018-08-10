package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

@CommandLine.Command(
        name = "ycsb-a",
        description = "YCSB workload with 10x 100 byte objects (0.5 get, 0.5 put)"
)
public class YcsbA extends AbstractYcsb {
    public YcsbA() {
        super("ycsb-a", 10, 100, 0.5f, 0.5f);
    }
}
