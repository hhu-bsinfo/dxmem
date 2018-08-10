package de.hhu.bsinfo.dxmem.cli;

import picocli.CommandLine;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookA;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookB;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookC;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookD;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookE;
import de.hhu.bsinfo.dxmem.benchmark.workload.FacebookF;
import de.hhu.bsinfo.dxmem.benchmark.workload.MemVar;
import de.hhu.bsinfo.dxmem.benchmark.workload.YcsbA;
import de.hhu.bsinfo.dxmem.benchmark.workload.YcsbB;
import de.hhu.bsinfo.dxmem.benchmark.workload.YcsbC;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterStorageUnit;
import de.hhu.bsinfo.dxmem.generated.BuildConfig;
import de.hhu.bsinfo.dxmonitor.info.InstanceInfo;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

@CommandLine.Command(
        name = "benchmark",
        customSynopsis = "@|bold dxmem benchmark|@ @|yellow heapSize WORKLOAD|@ [...]",
        description = "Run a benchmark to evaluate DXMem with different workloads",
        subcommands = {
                FacebookA.class,
                FacebookB.class,
                FacebookC.class,
                FacebookD.class,
                FacebookE.class,
                FacebookF.class,
                MemVar.class,
                YcsbA.class,
                YcsbB.class,
                YcsbC.class,
        }
)
public class ToolBenchmark implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            converter = TypeConverterStorageUnit.class,
            paramLabel = "heapSize",
            description = "Total heap size in bytes or StorageUnit")
    private StorageUnit m_heapSize;

    public void init() {
        printJVMArgs();
        printBuildInfo();
        printInstanceInfo();

        CliContext.getInstance().newMemory((short) 0, m_heapSize.getBytes());
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    /**
     * Print all JVM args specified on startup
     */
    private static void printJVMArgs() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> args = runtimeMxBean.getInputArguments();

        StringBuilder builder = new StringBuilder();
        builder.append("JVM arguments: ");

        for (String arg : args) {
            builder.append(arg);
            builder.append(' ');
        }

        System.out.println(builder);
        System.out.println();
    }

    /**
     * Print information about the current build
     */
    private static void printBuildInfo() {
        StringBuilder builder = new StringBuilder();

        builder.append(">>> DXMem build <<<\n");
        builder.append("Build type: ");
        builder.append(BuildConfig.BUILD_TYPE);
        builder.append('\n');
        builder.append("Git commit: ");
        builder.append(BuildConfig.GIT_COMMIT);
        builder.append('\n');
        builder.append("BuildDate: ");
        builder.append(BuildConfig.BUILD_DATE);
        builder.append('\n');
        builder.append("BuildUser: ");
        builder.append(BuildConfig.BUILD_USER);
        builder.append('\n');

        System.out.println(builder);
    }

    /**
     * Print hardware/software info about the current instance
     */
    private static void printInstanceInfo() {
        System.out.println(">>> Instance <<<\n" + InstanceInfo.compile() + '\n');
    }
}
