package task.common;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.executor.strategy.StrategyType;

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

        public final boolean startQpsUpdater;
        public final int minQps;
        public final int maxQps;
        public final int increaseQps;
        public final long timeDeltaQps;
        public final boolean testPoolUpdater;
        public final boolean testWorkerUpdater;

        public CommandConfig(boolean useThreadPool, int threads, String threadPoolStrategy,
                             int qps, boolean optimizeThreadPool, boolean optimizeWorkers,
                             int maxThreads, int maxWorkers, int minQueueCapacity,
                             int maxTotalQueueCapacity, boolean metrics,
                             boolean startQpsUpdater, int minQps,
                             int maxQps, int increaseQps,
                             long timeDeltaQps,
                             boolean testPoolUpdater, boolean testWorkerUpdater) {
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
            this.startQpsUpdater = startQpsUpdater;
            this.minQps = minQps;
            this.maxQps = maxQps;
            this.increaseQps = increaseQps;
            this.timeDeltaQps = timeDeltaQps;
            this.testPoolUpdater = testPoolUpdater;
            this.testWorkerUpdater = testWorkerUpdater;
        }
    }
    public static CommandConfig getCLIConfig(String[] args) throws ParseException {
        Options CommandLineOption = new Options();
        CommandLineOption.addOption("useThreadPool"   ,false, "useThreadPool");
        CommandLineOption.addOption("threads" ,true,  "threads");
        CommandLineOption.addOption("maxThreads" ,true,  "maxThreads");
        CommandLineOption.addOption("maxWorkers" ,true,  "maxWorkers");
        CommandLineOption.addOption("minQueueCapacity", true, "minQueueCapacity");
        CommandLineOption.addOption("maxTotalQueueCapacity", true, "maxTotalQueueCapacity");
        CommandLineOption.addOption("threadPoolStrategy", true, "threadPoolStrategy");
        CommandLineOption.addOption("qps", true, "qps");
        CommandLineOption.addOption("optimizeThreadPool", false, "optimizeThreadPool");
        CommandLineOption.addOption("optimizeWorkers", false, "optimizeWorkers");
        CommandLineOption.addOption("metrics", false, "metrics");
        CommandLineOption.addOption("minQps", true, "minQps");
        CommandLineOption.addOption("maxQps", true, "maxQps");
        CommandLineOption.addOption("increaseQps", true, "increaseQps");
        CommandLineOption.addOption("timeDeltaQps", true, "timeDeltaQps");
        CommandLineOption.addOption("testPoolUpdater", false, "testPoolUpdater");
        CommandLineOption.addOption("testWorkerUpdater", false, "testWorkerUpdater");

        DefaultParser parser = new DefaultParser();
        org.apache.commons.cli.CommandLine cli = parser.parse(CommandLineOption, args);

        boolean useThreadPool = cli.hasOption("useThreadPool");
        int threads = cli.hasOption("threads") ? Integer.parseInt(cli.getOptionValue("threads")) : 4;
        String threadPoolStrategy = cli.hasOption("threadPoolStrategy") ? cli.getOptionValue("threadPoolStrategy") : StrategyType.AD.name();
        int qps = cli.hasOption("qps") ? Integer.parseInt(cli.getOptionValue("qps")) : 1000;
        int maxThreads = cli.hasOption("maxThreads") ? Integer.parseInt(cli.getOptionValue("maxThreads")) : threads;
        int maxWorkers = cli.hasOption("maxWorkers") ? Integer.parseInt(cli.getOptionValue("maxWorkers")) : 1;
        int minQueueCapacity = cli.hasOption("minQueueCapacity") ? Integer.parseInt(cli.getOptionValue("minQueueCapacity")) : 5000;
        int maxTotalQueueCapacity = cli.hasOption("maxTotalQueueCapacity") ? Integer.parseInt(cli.getOptionValue("maxTotalQueueCapacity")) : 2000000;
        boolean optimizeThreadPool = cli.hasOption("optimizeThreadPool");
        boolean optimizeWorkers = cli.hasOption("optimizeWorkers");
        boolean metrics = cli.hasOption("metrics");
        boolean startQpsUpdater = cli.hasOption("minQps");
        int minQps = cli.hasOption("minQps") ? Integer.parseInt(cli.getOptionValue("minQps")) : 0;
        int maxQps = cli.hasOption("maxQps") ? Integer.parseInt(cli.getOptionValue("maxQps")) : 0;
        int increaseQps = cli.hasOption("increaseQps") ? Integer.parseInt(cli.getOptionValue("increaseQps")) : 0;
        long timeDeltaQps = cli.hasOption("timeDeltaQps") ? Long.parseLong(cli.getOptionValue("timeDeltaQps")) : 0;
        boolean testPoolUpdater = cli.hasOption("testPoolUpdater");
        boolean testWorkerUpdater = cli.hasOption("testWorkerUpdater");

        return new CommandConfig(useThreadPool, threads, threadPoolStrategy,
                qps, optimizeThreadPool, optimizeWorkers, maxThreads, maxWorkers,
                minQueueCapacity, maxTotalQueueCapacity, metrics, startQpsUpdater,
                minQps, maxQps, increaseQps, timeDeltaQps, testPoolUpdater, testWorkerUpdater);
    }
}
