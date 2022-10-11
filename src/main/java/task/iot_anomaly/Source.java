package task.iot_anomaly;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Source extends BaseRichSpout {
    private static Random RAND = new Random();
    private static List<String> allLines = getAllLines();

    private SpoutOutputCollector collector;
    private long sourceStartTime = -1;
    private final int qps;
    private int totalCount = 0;

    public Source(int qps) {
        this.qps = qps;
    }

    private static List<String> getAllLines() {
        List<String> allLines = null;
        try {
            allLines = Files.readAllLines(Paths.get(Config.csvPath), StandardCharsets.UTF_8);
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
    }

    @Override
    public void nextTuple() {
        long current = System.currentTimeMillis();
        if (sourceStartTime <= 0) {
            sourceStartTime = current;
        }
        int remains = qps;
        int cur = 0;
        int interval = remains / 10;
        for (int i = 0; i < 10; i++) {
            int end = i == 9 ? remains : cur + interval;
            while (cur < end) {
                cur++;
                int randType = RAND.nextInt(100);
                String type = "A"; // 60%
                if (60 <= randType && randType < 90) {
                    type = "B"; // 30%
                } else if (90 <= randType && randType < 99) {
                    type = "C"; // 9%
                } else if (randType == 99) {
                    type = "D"; // 1%
                }
                int len = RAND.nextInt(50) + 1;
                int start = RAND.nextInt(allLines.size() + 1 - len);
                List<String> data = new ArrayList<>(allLines.subList(start, start + len));
                collector.emit(new Values(System.currentTimeMillis(), type, data));
            }
            try {
                Thread.sleep(100 - (i / 9)); // the main logic may cost 1ms
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        totalCount += cur;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("start", "type", "data"));
    }
}
