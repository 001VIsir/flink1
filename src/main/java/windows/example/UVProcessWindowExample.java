package windows.example; // 声明包名，用于组织和管理类

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner; // 导入时间戳分配器接口
import org.apache.flink.api.common.eventtime.WatermarkStrategy; // 导入水印策略
import org.apache.flink.streaming.api.datastream.DataStreamSource; // 导入数据流源
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator; // 导入单输出流操作符
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment; // 导入流执行环境
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows; // 导入滚动事件时间窗口
import org.apache.flink.streaming.api.windowing.time.Time; // 导入时间类
import org.apache.flink.streaming.api.windowing.windows.TimeWindow; // 导入时间窗口类
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction; // 导入全窗口处理函数
import org.apache.flink.util.Collector; // 导入收集器接口

import java.sql.Timestamp; // 导入Timestamp类，用于格式化时间戳
import java.time.Duration; // 导入Duration类，用于表示时间间隔
import java.util.HashSet; // 导入HashSet类，用于存储唯一用户ID

public class UVProcessWindowExample { // 定义独立访客（UV）处理窗口示例类
    public static void main(String[] args) throws Exception { // 主方法，程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // 创建流执行环境
        env.setParallelism(1); // 设置并行度为1，确保结果有序
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource()); // 添加自定义数据源

        // 分配时间戳和水位线
        SingleOutputStreamOperator<Event> stream = streamSource
                .assignTimestampsAndWatermarks( // 分配时间戳和水位线
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO) // 使用零延迟的水位线策略（即不允许乱序）
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() { // 定义时间戳分配器
                                    @Override
                                    public long extractTimestamp(Event event, long l) { // 实现extractTimestamp方法
                                        return event.timestamp; // 从Event对象中提取时间戳
                                    }
                                })
                );

        // 全局窗口，统计10秒内UV
        stream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))) // 使用10秒的滚动事件时间窗口，无分组
                .process(new ProcessAllWindowFunction<Event, String, TimeWindow>() { // 定义窗口处理函数
                    @Override
                    public void process(Context context, Iterable<Event> elements, Collector<String> out) { // 处理窗口中的元素
                        HashSet<String> userSet = new HashSet<>(); // 创建HashSet用于存储唯一用户ID
                        for (Event event : elements) { // 遍历窗口中的所有事件
                            userSet.add(event.user); // 将用户ID添加到集合中（HashSet自动去重）
                        }

                        // 格式化输出窗口时间范围和独立访客数量
                        Long start = context.window().getStart(); // 获取窗口的开始时间
                        Long end = context.window().getEnd(); // 获取窗口的结束时间
                        Timestamp startTs = new Timestamp(start); // 将开始时间转换为Timestamp对象
                        Timestamp endTs = new Timestamp(end); // 将结束时间转换为Timestamp对象

                        out.collect(String.format("全窗口：%s~%s的独立访客数量是：%d", // 格式化输出字符串
                                startTs.toString().replace(' ', '0'), // 将时间戳中的空格替换为0，符合输出格式要求
                                endTs.toString().replace(' ', '0'), // 将时间戳中的空格替换为0，符合输出格式要求
                                userSet.size())); // 输出集合大小，即为独立访客数量
                    }
                })
                .print(); // 输出结果到控制台

        env.execute(); // 执行Flink作业
    }
}