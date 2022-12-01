package task.common;

import org.apache.storm.Config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
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

    public static void simulateFlowIncrease(AtomicInteger qps, long timeDelta, int[] qpsList) {
        Thread updateQpsThread = new Thread(() -> {
            qps.set(getQpsFromFile(CommonConfig.qpsFilePath, 0));
            System.out.printf("[simulateFlowIncrease] when start, qps=%d, timeDelta=%d, qpsList=%s\n",
                    qps.get(), timeDelta, Arrays.toString(qpsList));
            while (true) {
                try {
                    for (int val : qpsList) {
                        if (qps.get() > val) {
                            continue;
                        }
                        qps.set(val);
                        writeQpsToFile(CommonConfig.qpsFilePath, val);
                        Thread.sleep(timeDelta);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        updateQpsThread.setDaemon(true);
        updateQpsThread.start();
    }

    public static int getQpsFromFile(String path, int defaultVal) {
        try {
            if (!(new File(path)).exists()) {
                return defaultVal;
            }
            BufferedReader in = new BufferedReader(new FileReader(path));
            String str = in.readLine();
            return Integer.parseInt(str);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return defaultVal;
    }

    public static void writeQpsToFile(String path, int val) {
        try {
            File file =new File(path);
            FileWriter fileWriter = new FileWriter(file);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.write(Integer.toString(val));
            bufferWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
