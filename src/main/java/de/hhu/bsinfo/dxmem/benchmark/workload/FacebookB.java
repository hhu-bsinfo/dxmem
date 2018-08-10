package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

@CommandLine.Command(
        name = "facebook-b",
        description = "Facebook (memcached) workload with 1x 32 byte objects (0.95 get, 0.05 put)"
)
public class FacebookB extends AbstractFacebook {
    public FacebookB() {
        super("facebook-b", 1, 32, 0.95f, 0.05f);
    }
}
