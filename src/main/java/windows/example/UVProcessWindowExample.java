package windows.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class UVProcessWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        // 分配时间戳和水位线
        SingleOutputStreamOperator<Event> stream = streamSource
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                );

        // 全局窗口，统计10秒内UV
        stream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Event, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Event> elements, Collector<String> out) {
                        HashSet<String> userSet = new HashSet<>();
                        for (Event event : elements) {
                            userSet.add(event.user);
                        }

                        // 格式化输出窗口时间范围和独立访客数量
                        Long start = context.window().getStart();
                        Long end = context.window().getEnd();
                        Timestamp startTs = new Timestamp(start);
                        Timestamp endTs = new Timestamp(end);

                        out.collect(String.format("全窗口：%s~%s的独立访客数量是：%d",
                                startTs.toString().replace(' ', '0'),
                                endTs.toString().replace(' ', '0'),
                                userSet.size()));
                    }
                })
                .print();

        env.execute();
    }
}