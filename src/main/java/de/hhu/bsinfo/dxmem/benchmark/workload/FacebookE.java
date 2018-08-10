package de.hhu.bsinfo.dxmem.benchmark.workload;

import picocli.CommandLine;

@CommandLine.Command(
        name = "facebook-e",
        description = "Facebook graph workload with 1x 64 byte objects (0.5 get, 0.5 put)"
)
public class FacebookE extends AbstractFacebook {
    public FacebookE() {
        super("facebook-e", 1, 64, 0.5f, 0.5f);
    }
}
