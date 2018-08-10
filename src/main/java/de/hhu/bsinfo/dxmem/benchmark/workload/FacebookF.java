package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

@CommandLine.Command(
        name = "facebook-f",
        description = "Facebook graph workload with 1x 64 byte objects (0.95 get, 0.05 put)"
)
public class FacebookF extends AbstractFacebook {
    public FacebookF() {
        super("facebook-f", 1, 64, 0.95f, 0.05f);
    }
}
