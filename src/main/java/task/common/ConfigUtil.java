package task.common;

import org.apache.storm.Config;

import java.util.Arrays;
import java.util.List;

public class ConfigUtil {
    public static void updateConfig(Config conf, CommandLine.CommandConfig commandConfig, List<String> ids) {
        if (commandConfig.useThreadPool) {
            conf.useBoltThreadPool(true);
            conf.setBoltThreadPoolCoreConsumers(commandConfig.threads);
            conf.setTopologyBoltThreadPoolStrategy(commandConfig.threadPoolStrategy);
            conf.setTopologyBoltThreadPoolFetchMaxTasks(commandConfig.fetchMaxTasks);
            conf.enableBoltThreadPoolOptimize(commandConfig.optimizeThreadPool);
            conf.enableWorkersOptimize(commandConfig.optimizeWorkers);
            conf.setTopologyBoltThreadPoolIds(ids);
        }
    }
}
