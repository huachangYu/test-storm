package task.common;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.executor.ScheduledStrategy;

public class CommandLine {
    public static class CommandConfig {
        public final boolean useThreadPool;
        public final int threads;
        public final String threadPoolStrategy;
        public final int qps;
        public final boolean optimizeThreadPool;
        public final boolean optimizeWorkers;
        public final int maxThreads;
        public final int maxWorkers;
        public final int minQueueCapacity;
        public final int maxTotalQueueCapacity;
        public final boolean metrics;

        public CommandConfig(boolean useThreadPool, int threads, String threadPoolStrategy,
                             int qps, boolean optimizeThreadPool, boolean optimizeWorkers,
                             int maxThreads, int maxWorkers, int minQueueCapacity,
                             int maxTotalQueueCapacity, boolean metrics) {
            this.useThreadPool = useThreadPool;
            this.threads = threads;
            this.threadPoolStrategy = threadPoolStrategy;
            this.qps = qps;
            this.optimizeThreadPool = optimizeThreadPool;
            this.optimizeWorkers = optimizeWorkers;
            this.maxThreads = maxThreads;
            this.maxWorkers = maxWorkers;
            this.minQueueCapacity = minQueueCapacity;
            this.maxTotalQueueCapacity = maxTotalQueueCapacity;
            this.metrics = metrics;
        }
    }
    public static CommandConfig getCLIConfig(String[] args) throws ParseException {
        Options JDUL = new Options();
        JDUL.addOption("useThreadPool"   ,false, "useThreadPool");
        JDUL.addOption("threads" ,true,  "threads");
        JDUL.addOption("maxThreads" ,true,  "maxThreads");
        JDUL.addOption("maxWorkers" ,true,  "maxWorkers");
        JDUL.addOption("minQueueCapacity", true, "minQueueCapacity");
        JDUL.addOption("maxTotalQueueCapacity", true, "maxTotalQueueCapacity");
        JDUL.addOption("threadPoolStrategy", true, "threadPoolStrategy");
        JDUL.addOption("qps", true, "qps");
        JDUL.addOption("optimizeThreadPool", false, "optimizeThreadPool");
        JDUL.addOption("optimizeWorkers", false, "optimizeWorkers");
        JDUL.addOption("metrics", false, "metrics");

        DefaultParser parser = new DefaultParser();
        org.apache.commons.cli.CommandLine cli = parser.parse(JDUL, args);
        boolean useThreadPool = cli.hasOption("useThreadPool");
        int threads = cli.hasOption("threads") ? Integer.parseInt(cli.getOptionValue("threads")) : 4;
        String threadPoolStrategy = cli.hasOption("threadPoolStrategy") ? cli.getOptionValue("threadPoolStrategy") : ScheduledStrategy.Strategy.Fair.name();
        int qps = cli.hasOption("qps") ? Integer.parseInt(cli.getOptionValue("qps")) : 1000;
        int maxThreads = cli.hasOption("maxThreads") ? Integer.parseInt(cli.getOptionValue("maxThreads")) : 4;
        int maxWorkers = cli.hasOption("maxWorkers") ? Integer.parseInt(cli.getOptionValue("maxWorkers")) : 1;
        int minQueueCapacity = cli.hasOption("minQueueCapacity") ? Integer.parseInt(cli.getOptionValue("minQueueCapacity")) : 5000;
        int maxTotalQueueCapacity = cli.hasOption("maxTotalQueueCapacity") ? Integer.parseInt(cli.getOptionValue("maxTotalQueueCapacity")) : 2000000;
        boolean optimizeThreadPool = cli.hasOption("optimizeThreadPool");
        boolean optimizeWorkers = cli.hasOption("optimizeWorkers");
        boolean metrics = cli.hasOption("metrics");
        return new CommandConfig(useThreadPool, threads, threadPoolStrategy,
                qps, optimizeThreadPool, optimizeWorkers, maxThreads, maxWorkers,
                minQueueCapacity, maxTotalQueueCapacity, metrics);
    }
}
