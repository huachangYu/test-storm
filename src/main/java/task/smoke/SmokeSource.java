package task.smoke;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import task.common.CommonConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SmokeSource extends BaseRichSpout {
    private static Random RAND = new Random();
    private static List<String> allLines = getAllLines();

    private SpoutOutputCollector collector;
    private long sourceStartTime = -1;
    private AtomicInteger qps;
    private int maxQps;
    private int qpsIncrease;
    private long qpsTimeDelta;
    private Thread updateQpsThread;
    private int totalCount = 0;

    public SmokeSource(int minQps, int maxQps, int increase, long timeDelta) {
        this.qps = new AtomicInteger(minQps);
        this.maxQps = maxQps;
        this.qpsIncrease = increase;
        this.qpsTimeDelta = timeDelta;
    }


    public SmokeSource(int qps) {
        this.qps = new AtomicInteger(qps);
    }

    private static List<String> getAllLines() {
        List<String> allLines = null;
        try {
            allLines = Files.readAllLines(Paths.get(CommonConfig.smokeCsvPath), StandardCharsets.UTF_8);
            if (allLines.size() <= 1) {
                return new ArrayList<>();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return allLines.subList(1, allLines.size());
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.updateQpsThread = new Thread(() -> {
            while (qps.get() < maxQps) {
                try {
                    Thread.sleep(qpsTimeDelta);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                qps.getAndAdd(qpsIncrease);
                System.out.println("update qps to " + qps);
            }
        });
        this.updateQpsThread.setDaemon(true);
        this.updateQpsThread.start();
    }

    @Override
    public void nextTuple() {
        long current = System.currentTimeMillis();
        if (sourceStartTime <= 0) {
            sourceStartTime = current;
        }
        final AtomicInteger cur = new AtomicInteger();
        final int runTimes = 100;
        final int totalSize = qps.get();
        final int interval = totalSize / runTimes;
        final CountDownLatch countDownLatch =new CountDownLatch(runTimes);
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            int end = countDownLatch.getCount() == 1 ? totalSize : cur.get() + interval;
            while (cur.get() < end) {
                cur.getAndIncrement();
                int randType = RAND.nextInt(100);
                String type = "A"; // 60%
                if (60 <= randType && randType < 90) {
                    type = "B"; // 30%
                } else if (90 <= randType && randType < 99) {
                    type = "C"; // 9%
                } else if (randType == 99) {
                    type = "D"; // 1%
                }
                int len = RAND.nextInt(25) + 1;
                int start = RAND.nextInt(allLines.size() + 1 - len);
                List<String> data = new ArrayList<>(allLines.subList(start, start + len));
                collector.emit(new Values(System.currentTimeMillis(), type, data));
            }
            countDownLatch.countDown();
        }, 0, 1000 / runTimes, TimeUnit.MILLISECONDS);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        executor.shutdown();
        totalCount += totalSize;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("start", "type", "data"));
    }
}
