/**
 * for debug
 * remove it when releasing
 * */

package task;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import task.common.CommonConfig;

public class WordCountTopology {
    public static class RandomSentenceSpout extends BaseRichSpout {
        private static final String[] sentences = new String[]{
                "an an an an an an an an an an",
                "an an an an an an an an an an",
                "an an an an an an an an an an",
                "an an an an an an an an an an",
                "i have more than four score and seven years ago"};
        private SpoutOutputCollector collector;
        private Random rand;
        private final AtomicInteger qps;
        private final int endQps;
        private final int increaseQps;
        private final int timeDelta;
        public RandomSentenceSpout(int qps, int endQps, int increaseQps, int timeDelta) {
            this.qps = new AtomicInteger(qps);
            this.endQps = endQps;
            this.increaseQps = increaseQps;
            this.timeDelta = timeDelta;
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            rand = new Random();
            new Thread(() -> {
                while (qps.get() < endQps) {
                    try {
                        Thread.sleep(timeDelta);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    qps.getAndAdd(increaseQps);
                }
            }).start();
        }

        @Override
        public void nextTuple() {
            final AtomicInteger cur = new AtomicInteger();
            final int runTimes = 100;
            final int totalSize = qps.get();
            final int interval = totalSize / runTimes;
            final CountDownLatch countDownLatch =new CountDownLatch(runTimes);
            final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
            executor.scheduleAtFixedRate(() -> {
                int end = countDownLatch.getCount() == 1 ? totalSize : cur.get() + interval;
                while (cur.get() < end) {
                    cur.getAndIncrement();
                    String sentence = sentences[rand.nextInt(sentences.length)];
                    collector.emit(new Values(System.currentTimeMillis(), sentence));
                }
                countDownLatch.countDown();
            }, 0, 1000 / runTimes, TimeUnit.MILLISECONDS);
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            executor.shutdown();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("startTime", "word"));
        }
    }

    public static class SplitSentence extends BaseBasicBolt {
        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            long start = input.getLongByField("startTime");
            String sentence = input.getStringByField("word");
            String[] words = sentence.split(" ");
            for (String w : words) {
                collector.emit(new Values(start, w));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("startTime", "word"));
        }
    }

    public static class WordCount extends BaseBasicBolt {
        private final Map<String, Long> counts = new HashMap<>();

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            long start = input.getLongByField("startTime");
            String word = input.getStringByField("word");
            Long count = counts.getOrDefault(word, 0L);
            count += 1;
            counts.put(word, count);
            collector.emit(new Values(start, word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("startTime", "word", "count"));
        }
    }

    public static class Output extends BaseBasicBolt {
        private long boltStartTime = -1;
        private long preTime = -1;
        private long periodCount = 0;
        private long periodCost = 0;
        private CentralProcessor systemInfoProcessor;
        private long[] oldTicks;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context) {
            super.prepare(topoConf, context);
            SystemInfo systemInfo = new SystemInfo();
            this.systemInfoProcessor = systemInfo.getHardware().getProcessor();
            this.oldTicks = this.systemInfoProcessor.getSystemCpuLoadTicks();
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            long current = System.currentTimeMillis();
            if (boltStartTime <= 0) {
                boltStartTime = current;
                preTime = current;
            }
            long start = input.getLongByField("startTime");
            long cost = current - start;
            periodCount++;
            periodCost += cost;
            if (current - preTime >= 1000) {
                long[] ticks = this.systemInfoProcessor.getSystemCpuLoadTicks();
                long total = 0;
                for (int i = 0; i < ticks.length; i++) {
                    total += ticks[i] - oldTicks[i];
                }
                long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] - oldTicks[CentralProcessor.TickType.IDLE.getIndex()];
                double cpuUsage = total <= 0 ? 0 : 1 - (double) idle / (double) total;
                System.out.printf("[Output-Throughput] time=%d, periodCnt=%d, periodAvgCost=%.2f, cpu=%.2f\n",
                        current - boltStartTime,
                        periodCount,
                        (double) periodCost / (double) periodCount,
                        cpuUsage * 100);
                periodCost = 0;
                periodCount = 0;
                preTime = current;
                oldTicks = ticks;
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) throws Exception {
        Options JDUL = new Options();
        JDUL.addOption("startQps", true, "startQps");
        JDUL.addOption("endQps", true, "endQps");
        JDUL.addOption("increaseQps", true, "increaseQps");
        JDUL.addOption("timeDelta", true, "timeDelta");
        JDUL.addOption("parallelism", true, "parallelism");
        DefaultParser parser = new DefaultParser();
        org.apache.commons.cli.CommandLine cli = parser.parse(JDUL, args);
        int startQps = cli.hasOption("startQps") ? Integer.parseInt(cli.getOptionValue("startQps")) : 1000;
        int endQps = cli.hasOption("endQps") ? Integer.parseInt(cli.getOptionValue("endQps")) : 100000;
        int increaseQps = cli.hasOption("increaseQps") ? Integer.parseInt(cli.getOptionValue("increaseQps")) : 1000;
        int timeDelta = cli.hasOption("timeDelta") ? Integer.parseInt(cli.getOptionValue("timeDelta")) : 60 * 1000;
        int parallelism = cli.hasOption("parallelism") ? Integer.parseInt(cli.getOptionValue("parallelism")) : 1;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(startQps, endQps, increaseQps, timeDelta), 1);
        builder.setBolt("split", new SplitSentence(), parallelism).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), parallelism).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("output", new Output(), 1).shuffleGrouping("count");

        Config conf = new Config();

        if (CommonConfig.isLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wineBenchmark", conf, builder.createTopology());
        } else {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
}