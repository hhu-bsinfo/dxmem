package de.hhu.bsinfo.dxmem;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import de.hhu.bsinfo.dxmem.core.Analyzer;
import de.hhu.bsinfo.dxmem.core.MemoryLoader;
import de.hhu.bsinfo.dxmem.generated.BuildConfig;

public class MemDumpAnalyzer {
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
        
        // Parse command line arguments
        if (p_args.length < 4) {
            System.out.println("Provide memory dump file to load and analyze.");
            System.out.println("Args: <path mem dump> <print status: 0/1> <analyze: 0/1> <verbose: 0/1>");
            System.out.println("Select one of the following benchmarks:");

            System.exit(-1);
        }

        String inputFile = p_args[0];
        boolean printStatus = Integer.parseInt(p_args[1]) > 0;
        boolean analyze = Integer.parseInt(p_args[2]) > 0;
        boolean verbose = Integer.parseInt(p_args[3]) > 0;

        MemoryLoader loader = new MemoryLoader();

        System.out.println("Loading memory dump from file " + inputFile + "...");

        try {
            loader.load(inputFile);
        } catch (Throwable e) {
            System.out.println("Loading failed: " + e.getMessage());
            System.exit(-1);
        }

        if (printStatus) {
            System.out.println("Status");
            System.out.println("========================== CIDTable ==========================");
            System.out.println(loader.getCIDTable());
            System.out.println("========================== Heap ==========================");
            System.out.println(loader.getHeap());
        }

        if (analyze) {
            // force trace to get detailed output
            if (verbose) {
                Configurator.setRootLevel(Level.TRACE);
            }

            System.out.println("Analyzing memory dump (this may take a while) ...");

            Analyzer analyzer = new Analyzer(loader.getHeap(), loader.getCIDTable());

            boolean ret = analyzer.analyze();

            // force flush before printing result
            LogManager.shutdown();

            if (!ret) {
                System.out.println("Analyzing memory dump not successful, errors detected (see logger output)");
            } else {
                System.out.println("Analyzing memory dump successful, no errors detected");
            }
        }

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
}
