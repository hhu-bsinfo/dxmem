package de.hhu.bsinfo.dxmem.benchmark.workload;

public class YcsbC extends AbstractYcsb {
    public YcsbC() {
        super(10, 100, 1.0f, 0.0f);
    }

    @Override
    public String getName() {
        return "ycsb-c";
    }

    @Override
    public String getDescription() {
        return "YCSB workload a with 10x 100 byte objects (100% get)";
    }
}
