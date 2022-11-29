package task.common;

import org.apache.storm.Config;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ConfigUtil {
    public static void updateConfig(Config conf, CommandLine.CommandConfig commandConfig, List<String> ids) {
        if (commandConfig.useThreadPool) {
            conf.useExecutorPool(true);
            conf.setExecutorPoolCoreConsumers(commandConfig.threads);
            conf.setExecutorPoolMaxConsumers(commandConfig.maxThreads);
            conf.setExecutorPoolStrategy(commandConfig.threadPoolStrategy);
            conf.enableExecutorPoolOptimize(commandConfig.optimizeThreadPool);
            conf.enableWorkersOptimize(commandConfig.optimizeWorkers);
            conf.setExecutorPoolMaxWorkerNum(commandConfig.maxWorkers);
            conf.setExecutorPoolMinQueueCapacity(commandConfig.minQueueCapacity);
            conf.setExecutorPoolTotalQueueCapacity(commandConfig.maxTotalQueueCapacity);
            conf.enableExecutorPoolPrintMetrics(commandConfig.metrics);
            conf.setExecutorPoolIds(ids);
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

    public static void simulateFlowSurge(AtomicInteger qps, long normalTimeDelta, long highTimeDelta,
                                         int normalQps, int[] highQpsList) {
        Thread updateQpsThread = new Thread(() -> {
            int status = 0;
            int n = 2 * highQpsList.length;
            while (true) {
                try {
                    if (status % 2 == 0) {
                        qps.set(normalQps);
                        Thread.sleep(normalTimeDelta);
                    } else {
                        qps.set(highQpsList[(status - 1) / 2]);
                        Thread.sleep(highTimeDelta);
                    }
                    status = (status + 1) % n;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        updateQpsThread.setDaemon(true);
        updateQpsThread.start();
    }
}
