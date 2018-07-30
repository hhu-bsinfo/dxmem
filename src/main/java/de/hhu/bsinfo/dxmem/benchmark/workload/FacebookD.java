package de.hhu.bsinfo.dxmem.benchmark.workload;

public class FacebookD extends AbstractFacebook {
    public FacebookD() {
        super(24, 32, 0.95f, 0.05f);
    }

    @Override
    public String getName() {
        return "facebook-d";
    }

    @Override
    public String getDescription() {
        return "Facebook (memcached) workload with 24x 32 byte objects (95% get, 5% put)";
    }
}
