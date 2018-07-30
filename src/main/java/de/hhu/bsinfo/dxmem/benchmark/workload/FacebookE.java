package de.hhu.bsinfo.dxmem.benchmark.workload;

public class FacebookE extends AbstractFacebook {
    public FacebookE() {
        super(1, 64, 0.5f, 0.5f);
    }

    @Override
    public String getName() {
        return "facebook-e";
    }

    @Override
    public String getDescription() {
        return "Facebook graph workload with 1x 64 byte objects (50% get, 50% put)";
    }
}
