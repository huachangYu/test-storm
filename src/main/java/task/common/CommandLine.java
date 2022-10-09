package task.common;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.executor.bolt.BoltWeightCalc;

public class CommandLine {
    public static class CommandConfig {
        public final boolean useThreadPool;
        public final int threads;
        public final String threadPoolStrategy;
        public final int fetchMaxTasks;
        public final int qps;
        public final boolean optimizeThreadPool;
        public final boolean optimizeWorkers;

        public CommandConfig(boolean useThreadPool, int threads, String threadPoolStrategy,
                             int fetchMaxTasks, int qps, boolean optimizeThreadPool, boolean optimizeWorkers) {
            this.useThreadPool = useThreadPool;
            this.threads = threads;
            this.threadPoolStrategy = threadPoolStrategy;
            this.fetchMaxTasks = fetchMaxTasks;
            this.qps = qps;
            this.optimizeThreadPool = optimizeThreadPool;
            this.optimizeWorkers = optimizeWorkers;
        }
    }
    public static CommandConfig getCLIConfig(String[] args) throws ParseException {
        Options JDUL = new Options();
        JDUL.addOption("useThreadPool"   ,false, "useThreadPool");
        JDUL.addOption("threads" ,true,  "threads");
        JDUL.addOption("threadPoolStrategy", true, "threadPoolStrategy");
        JDUL.addOption("qps", true, "qps");
        JDUL.addOption("fetchMaxTasks", true, "fetchMaxTasks");
        JDUL.addOption("optimizeThreadPool", false, "optimizeThreadPool");
        JDUL.addOption("optimizeWorkers", false, "optimizeWorkers");

        DefaultParser parser = new DefaultParser();
        org.apache.commons.cli.CommandLine cli = parser.parse(JDUL, args);
        boolean useThreadPool = cli.hasOption("useThreadPool");
        int threads = cli.hasOption("threads") ? Integer.parseInt(cli.getOptionValue("threads")) : 4;
        String threadPoolStrategy = cli.hasOption("threadPoolStrategy") ? cli.getOptionValue("threadPoolStrategy") : BoltWeightCalc.Strategy.Fair.name();
        int fetchMaxTasks = cli.hasOption("fetchMaxTasks") ? Integer.parseInt(cli.getOptionValue("fetchMaxTasks")) : 1;
        int qps = cli.hasOption("qps") ? Integer.parseInt(cli.getOptionValue("qps")) : 1000;
        boolean optimizeThreadPool = cli.hasOption("optimizeThreadPool");
        boolean optimizeWorkers = cli.hasOption("optimizeWorkers");
        return new CommandConfig(useThreadPool, threads, threadPoolStrategy, fetchMaxTasks,
                qps, optimizeThreadPool, optimizeWorkers);
    }
}
