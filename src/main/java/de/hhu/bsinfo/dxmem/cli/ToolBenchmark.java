/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxmem.cli;

import de.hhu.bsinfo.dxmem.benchmark.workload.*;
import picocli.CommandLine;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

import de.hhu.bsinfo.dxmem.benchmark.AbstractLocalBenchmarkRunner;
import de.hhu.bsinfo.dxmem.benchmark.DXMemBenchmarkContext;
import de.hhu.bsinfo.dxmem.cli.types.TypeConverterStorageUnit;
import de.hhu.bsinfo.dxmem.generated.BuildConfig;
import de.hhu.bsinfo.dxmonitor.info.InstanceInfo;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

/**
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
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
                YcsbCustom.class
        }
)
public class ToolBenchmark extends AbstractLocalBenchmarkRunner implements Runnable {
    @CommandLine.Parameters(
            index = "0",
            converter = TypeConverterStorageUnit.class,
            paramLabel = "heapSize",
            description = "Total heap size in bytes or StorageUnit")
    private StorageUnit m_heapSize;

    @CommandLine.Parameters(
            index = "1",
            paramLabel = "disableChunkLocks",
            description = "Disable the chunk locks (see DXMem class documentation for details), set to false for " +
                    "default behaviour")
    private boolean m_disableChunkLocks = false;

    /**
     * Constructor
     */
    public ToolBenchmark() {
        super(new DXMemBenchmarkContext());
    }

    @Override
    public boolean init() {
        printJVMArgs();
        printBuildInfo();
        printInstanceInfo();

        CliContext.getInstance().newMemory((short) 0, m_heapSize.getBytes(), m_disableChunkLocks);

        return true;
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
