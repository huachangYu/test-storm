package task.common;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.executor.bolt.BoltWeightCalc;

public class CommandLine {
    public static class CommandConfig {
        public boolean useThreadPool;
        public int threads;
        public String threadPoolStrategy;
        public int fetchMaxTasks;
        public int qps;

        public CommandConfig(boolean useThreadPool, int threads, String threadPoolStrategy,
                             int fetchMaxTasks, int qps) {
            this.useThreadPool = useThreadPool;
            this.threads = threads;
            this.threadPoolStrategy = threadPoolStrategy;
            this.fetchMaxTasks = fetchMaxTasks;
            this.qps = qps;
        }
    }
    public static CommandConfig getCLIConfig(String[] args) throws ParseException {
        Options JDUL = new Options();
        JDUL.addOption("useThreadPool"   ,false, "useThreadPool");
        JDUL.addOption("threads" ,true,  "threads");
        JDUL.addOption("threadPoolStrategy", true, "threadPoolStrategy");
        JDUL.addOption("qps", true, "qps");
        JDUL.addOption("fetchMaxTasks", true, "fetchMaxTasks");
        JDUL.addOption("shuffle", false, "shuffle");

        DefaultParser parser = new DefaultParser();
        org.apache.commons.cli.CommandLine cli = parser.parse(JDUL, args);
        boolean useThreadPool = cli.hasOption("useThreadPool");
        int threads = cli.hasOption("threads") ? Integer.parseInt(cli.getOptionValue("threads")) : 4;
        String threadPoolStrategy = cli.hasOption("threadPoolStrategy") ? cli.getOptionValue("threadPoolStrategy") : BoltWeightCalc.Strategy.Fair.name();
        int fetchMaxTasks = cli.hasOption("fetchMaxTasks") ? Integer.parseInt(cli.getOptionValue("fetchMaxTasks")) : 1;
        int qps = cli.hasOption("qps") ? Integer.parseInt(cli.getOptionValue("qps")) : 1000;
        return new CommandConfig(useThreadPool, threads, threadPoolStrategy, fetchMaxTasks, qps);
    }
}
