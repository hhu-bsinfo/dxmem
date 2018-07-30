package de.hhu.bsinfo.dxmem.benchmark.workload;

public class FacebookA extends AbstractFacebook {
    public FacebookA() {
        super(1, 32, 0.5f, 0.5f);
    }

    @Override
    public String getName() {
        return "facebook-a";
    }

    @Override
    public String getDescription() {
        return "Facebook (memcached) workload with 1x 32 byte objects (50% get, 50% put)";
    }
}
