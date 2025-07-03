package windows.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Iterator;

public class UvCountByWindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 数据源
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        // 2. 时间戳与水位线（假设有序）
        SingleOutputStreamOperator<Event> stream = streamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long recordTimestamp) {
                                return event.timestamp;
                            }
                        }));

        // 3. 使用 WindowFunction 实现 UV 统计
        stream
                .keyBy(event -> true)  // 所有数据到一个 key 分组
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new UVWindowFunction())
                .print("WindowFunction");

        // 4. 使用 ProcessWindowFunction 实现 UV 统计
        stream
                .keyBy(event -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UVProcessWindowFunction())
                .print("ProcessWindowFunction");

        env.execute();
    }

    // 全窗口函数实现 UV
    public static class UVWindowFunction implements WindowFunction<Event, String, Boolean, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {
        @Override
        public void apply(Boolean key, org.apache.flink.streaming.api.windowing.windows.TimeWindow window, Iterable<Event> input, Collector<String> out) {
            HashSet<String> userSet = new HashSet<>();
            for (Event event : input) {
                userSet.add(event.user);
            }
            String windowStart = format(window.getStart());
            String windowEnd = format(window.getEnd());
            out.collect("全窗口:" + windowStart + "~" + windowEnd + " 的独立访客数量是:" + userSet.size());
        }
    }

    // ProcessWindowFunction 实现 UV
    public static class UVProcessWindowFunction extends ProcessWindowFunction<Event, String, Boolean, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {
        @Override
        public void process(Boolean key, Context context, Iterable<Event> elements, Collector<String> out) {
            HashSet<String> userSet = new HashSet<>();
            for (Event event : elements) {
                userSet.add(event.user);
            }
            String windowStart = format(context.window().getStart());
            String windowEnd = format(context.window().getEnd());
            out.collect("全窗口:" + windowStart + "~" + windowEnd + " 的独立访客数量是:" + userSet.size());
        }
    }

    // 时间戳格式化工具
    private static String format(long ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(ts);
    }
}
