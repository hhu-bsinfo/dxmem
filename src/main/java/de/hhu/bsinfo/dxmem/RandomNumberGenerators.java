package de.hhu.bsinfo.dxmem;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.security.SecureRandom;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.SplittableRandom;

import de.hhu.bsinfo.dxmem.generated.BuildConfig;
import de.hhu.bsinfo.dxmonitor.info.InstanceInfo;
import de.hhu.bsinfo.dxutils.stats.TimePercentilePool;

public class RandomNumberGenerators {
    public static void main(final String[] p_args) {
        Locale.setDefault(new Locale("en", "US"));
        printJVMArgs();
        printCmdArgs(p_args);
        printBuildInfo();

        if (p_args.length < 2) {
            System.out.println("Args: <operations> <threads>");
            System.exit(-1);
        }

        printInstanceInfo();

        execute();

        System.exit(0);
    }

    public static void execute() {
        // TODO benchmark different data types?
    }

    private static abstract class GeneratorBenchmark {
        private final Generator m_generator;
        private final long m_operations;

        private final TimePercentilePool m_time;
        private final Thread[] m_threads;

        public GeneratorBenchmark(final Generator p_generator, final long p_operations, final int p_threads) {
            m_generator = p_generator;
            m_operations = p_operations;

            m_threads = new Thread[p_threads];
            m_time = new TimePercentilePool(getClass(), m_generator.getName());
        }

        public void execute() {
            long opsPerThread = m_operations / m_threads.length;

            for (int i = 0; i < m_threads.length; i++) {
                m_threads[i] = new Thread(() -> {
                    for (int j = 0; j < opsPerThread; j++) {
                        long time = System.nanoTime();

                        m_generator.generateDouble();

                        m_time.record(System.nanoTime() - time);
                    }
                });
            }

            for (Thread t : m_threads) {
                t.start();
            }

            for (Thread t : m_threads) {
                try {
                    t.join();
                } catch (InterruptedException ignored) {
                }
            }

            m_time.sortValues();
        }

        public void printResults() {

        }
    }

    private interface Generator {
        String getName();

        double generateDouble();
    }

    private static class GeneratorMathRandom implements Generator {
        @Override
        public String getName() {
            return "MathRandom";
        }

        @Override
        public double generateDouble() {
            return Math.random();
        }
    }

    private static class GeneratorThreadLocalRandom implements Generator {
        @Override
        public String getName() {
            return "ThreadLocalRandom";
        }

        @Override
        public double generateDouble() {
            return java.util.concurrent.ThreadLocalRandom.current().nextDouble();
        }
    }

    private static class GeneratorUtilRandom implements Generator {
        private final Random rand = new Random();

        @Override
        public String getName() {
            return "UtilRandom";
        }

        @Override
        public double generateDouble() {
            return rand.nextDouble();
        }
    }

    private static class GeneratorSplittableRandom implements Generator {
        private final SplittableRandom rand = new SplittableRandom();

        @Override
        public String getName() {
            return "SplittableRandom";
        }

        @Override
        public double generateDouble() {
            return rand.nextDouble();
        }
    }

    private static class GeneratorSecureRandom implements Generator {
        private final SecureRandom rand = new SecureRandom();

        @Override
        public String getName() {
            return "SecureRandom";
        }

        @Override
        public double generateDouble() {
            return rand.nextDouble();
        }
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

        builder.append(">>> RandomNumberGenerator benchmark build <<<\n");
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
