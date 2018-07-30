package de.hhu.bsinfo.dxmem.benchmark.workload;

public class FacebookB extends AbstractFacebook {
    public FacebookB() {
        super(1, 32, 0.95f, 0.05f);
    }

    @Override
    public String getName() {
        return "facebook-b";
    }

    @Override
    public String getDescription() {
        return "Facebook (memcached) workload with 1x 32 byte objects (95% get, 5% put)";
    }
}
