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

package de.hhu.bsinfo.dxmem;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import de.hhu.bsinfo.dxmem.benchmark.Benchmark;
import de.hhu.bsinfo.dxmem.benchmark.workload.Workload;
import de.hhu.bsinfo.dxmem.benchmark.workload.YcsbA;
import de.hhu.bsinfo.dxmem.generated.BuildConfig;
import de.hhu.bsinfo.dxmonitor.info.InstanceInfo;

/**
 * DXMem benchmark and test application. Offers a selection of various (local only) benchmarks to evaluate
 * throughput and latency of the operations offered by DXNet.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.07.2018
 */
public class DXMemMain {
    private static final Map<String, Workload> ms_workloads = new HashMap<>();

    /**
     * Application entry point
     *
     * @param p_args Cmd args
     */
    public static void main(final String[] p_args) {
        Locale.setDefault(new Locale("en", "US"));
        printJVMArgs();
        printCmdArgs(p_args);
        printBuildInfo();

        initWorkloads();

        // Parse command line arguments
        if (p_args.length < 1) {
            System.out.println("No benchmark selected.");
            System.out.println("Args: <benchmark name> ...");
            System.out.println("Select one of the following benchmarks:");

            for (Map.Entry<String, Workload> entry : ms_workloads.entrySet()) {
                System.out.println("  " + entry.getKey() + ": " + entry.getValue().getDescription());
            }

            System.exit(-1);
        }

        Workload workload = ms_workloads.get(p_args[0]);

        if (workload == null) {
            System.out.println("Invalid workload '" + p_args[0] + "' specified. Available workloads:");

            for (Map.Entry<String, Workload> entry : ms_workloads.entrySet()) {
                System.out.println("  " + entry.getKey() + ": " + entry.getValue().getDescription());
            }

            System.exit(-1);
        }

        Benchmark benchmark = workload.createWorkload(Arrays.copyOfRange(p_args, 1, p_args.length));

        if (benchmark == null) {
            System.exit(-1);
        }

        printInstanceInfo();

        benchmark.execute();

        System.exit(0);
    }

    /**
     * Print all cmd args specified on startup
     *
     * @param p_args
     *         Main arguments
     */
    private static void printCmdArgs(final String[] p_args) {
        StringBuilder builder = new StringBuilder();
        builder.append("Cmd arguments: ");

        for (String arg : p_args) {
            builder.append(arg);
            builder.append(' ');
        }

        System.out.println(builder);
        System.out.println();
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

    /**
     * Add all selectable workloads to the list
     */
    private static void initWorkloads() {
        addWorkload(new YcsbA());
    }

    /**
     * Add a single workload to the workload list
     *
     * @param p_workload Workload to add
     */
    private static void addWorkload(final Workload p_workload) {
        ms_workloads.put(p_workload.getName(), p_workload);
    }
}
