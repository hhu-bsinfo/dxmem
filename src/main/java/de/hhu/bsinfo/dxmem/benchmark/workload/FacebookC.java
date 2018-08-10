package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

@CommandLine.Command(
        name = "facebook-c",
        description = "Facebook (memcached) workload with 24x 32 byte objects (0.5 get, 0.5 put)"
)
public class FacebookC extends AbstractFacebook {
    public FacebookC() {
        super("facebook-c", 24, 32, 0.5f, 0.5f);
    }
}
