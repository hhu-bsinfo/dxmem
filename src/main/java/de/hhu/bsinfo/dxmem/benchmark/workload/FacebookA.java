package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

@CommandLine.Command(
        name = "facebook-a",
        description = "Facebook (memcached) workload with 1x 32 byte objects (0.5 get, 0.5 put)"
)
public class FacebookA extends AbstractFacebook {
    public FacebookA() {
        super("facebook-a", 1, 32, 0.5f, 0.5f);
    }
}
