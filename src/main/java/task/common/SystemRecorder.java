package task.common;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

public class SystemRecorder {
    private CentralProcessor systemInfoProcessor;
    private long[] oldTicks;

    public SystemRecorder() {
        SystemInfo systemInfo = new SystemInfo();
        this.systemInfoProcessor = systemInfo.getHardware().getProcessor();
        this.oldTicks = this.systemInfoProcessor.getSystemCpuLoadTicks();
    }

    public double getAndRecordCpuLoad() {
        long[] ticks = this.systemInfoProcessor.getSystemCpuLoadTicks();
        long total = 0;
        for (int i = 0; i < ticks.length; i++) {
            total += ticks[i] - oldTicks[i];
        }
        long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] - oldTicks[CentralProcessor.TickType.IDLE.getIndex()];
        double cpuUsage = total <= 0 ? 0 : 1 - (double) idle / (double) total;
        oldTicks = ticks;
        return cpuUsage;
    }
}
