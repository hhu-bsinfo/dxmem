package de.hhu.bsinfo.dxmem.benchmark.workload;

public class YcsbB extends AbstractYcsb {
    public YcsbB() {
        super(10, 100, 0.95f, 0.05f);
    }

    @Override
    public String getName() {
        return "ycsb-b";
    }

    @Override
    public String getDescription() {
        return "YCSB workload a with 10x 100 byte objects (95% get, 5% put)";
    }
}
