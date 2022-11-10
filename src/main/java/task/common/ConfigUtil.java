package task.common;

import org.apache.storm.Config;

import java.util.Arrays;
import java.util.List;

public class ConfigUtil {
    public static void updateConfig(Config conf, CommandLine.CommandConfig commandConfig, List<String> ids) {
        if (commandConfig.useThreadPool) {
            conf.useBoltExecutorPool(true);
            conf.setBoltExecutorPoolCoreConsumers(commandConfig.threads);
            conf.setBoltExecutorPoolMaxConsumers(commandConfig.maxThreads);
            conf.setBoltExecutorPoolStrategy(commandConfig.threadPoolStrategy);
            conf.enableBoltExecutorPoolOptimize(commandConfig.optimizeThreadPool);
            conf.enableWorkersOptimize(commandConfig.optimizeWorkers);
            conf.setBoltExecutorPoolMaxWorkerNum(commandConfig.maxWorkers);
            conf.setBoltExecutorPoolMinQueueCapacity(commandConfig.minQueueCapacity);
            conf.setBoltExecutorPoolTotalQueueCapacity(commandConfig.maxTotalQueueCapacity);
            conf.enableBoltExecutorPoolPrintMetrics(commandConfig.metrics);
            conf.setBoltExecutorPoolIds(ids);
        }
    }
}
