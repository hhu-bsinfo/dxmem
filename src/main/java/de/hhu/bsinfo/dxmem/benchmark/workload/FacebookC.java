package de.hhu.bsinfo.dxmem.benchmark.workload;

public class FacebookC extends AbstractFacebook {
    public FacebookC() {
        super(24, 32, 0.5f, 0.5f);
    }

    @Override
    public String getName() {
        return "facebook-c";
    }

    @Override
    public String getDescription() {
        return "Facebook (memcached) workload with 24x 32 byte objects (50% get, 50% put)";
    }
}
