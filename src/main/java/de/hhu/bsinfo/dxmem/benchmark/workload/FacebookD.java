package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

@CommandLine.Command(
        name = "facebook-d",
        description = "Facebook (memcached) workload with 24x 32 byte objects (0.95 get, 0.05 put)"
)
public class FacebookD extends AbstractFacebook {
    public FacebookD() {
        super("facebook-d", 24, 32, 0.95f, 0.05f);
    }
}
