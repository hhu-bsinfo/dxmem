package de.hhu.bsinfo.dxmem.benchmark.workload;

public class FacebookF extends AbstractFacebook {
    public FacebookF() {
        super(1, 64, 0.95f, 0.05f);
    }

    @Override
    public String getName() {
        return "facebook-f";
    }

    @Override
    public String getDescription() {
        return "Facebook graph workload with 1x 64 byte objects (95% get, 5% put)";
    }
}
