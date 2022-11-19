package task.common;

import org.apache.storm.Config;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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

    public static void startIncreasingQpsThread(AtomicInteger qps, int endQps,
                                         int increaseQps, long timeDelta) {
        Thread updateQpsThread = new Thread(() -> {
            while (qps.get() < endQps) {
                try {
                    Thread.sleep(timeDelta);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                qps.getAndAdd(increaseQps);
                System.out.println("update qps to " + qps);
            }
        });
        updateQpsThread.setDaemon(true);
        updateQpsThread.start();
    }
}
