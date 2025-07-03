package windows.example;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

public class WindowAggregateExample {
    /**
     * 统计 10 秒钟的“人均 PV”，每 2 秒统计一次 （增量聚合）-》聚合函数（聚合状态的类型、输出结果的类型都必须和输入数据类型一样）
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        //乱序
        SingleOutputStreamOperator<Event> stream =streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.timestamp;
            }
        }));
//        所有数据设置相同的 key，发送到同一个分区统计 PV 和 UV，再相除
        stream.keyBy(x -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
                .aggregate(new AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double>() {

                    @Override
                    public Tuple2<HashSet<String>, Long> createAccumulator() {
                        // 创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次。
                        return Tuple2.of(new HashSet<String>(), 0L);
                    }

                    @Override
                    public Tuple2<HashSet<String>, Long> add(Event event, Tuple2<HashSet<String>, Long> accumulator) {
                        // 属于本窗口的数据来一条累加一次，并返回累加器
                        accumulator.f0.add(event.user);
                        return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
                    }

                    @Override
                    public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
                        //从累加器中提取聚合的输出结果
                        return  (double)accumulator.f1/accumulator.f0.size();
                    }

                    @Override
                    public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> hashSetLongTuple2, Tuple2<HashSet<String>, Long> acc1) {
//                        合并两个累加器，并将合并后的状态作为一个累加器返回。这个方法只在需要合并窗口的场景下才会被调用
                        return null;
                    }
                })
                .print("人均pv:");
        env.execute();
    }
}
