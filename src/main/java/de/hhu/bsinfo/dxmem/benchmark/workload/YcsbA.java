package de.hhu.bsinfo.dxmem.benchmark.workload;

public class YcsbA extends AbstractYcsb {
    public YcsbA() {
        super(10, 100, 0.5f, 0.5f);
    }

    @Override
    public String getName() {
        return "ycsb-a";
    }

    @Override
    public String getDescription() {
        return "YCSB workload a with 10x 100 byte objects (50% get, 50% put)";
    }

}
