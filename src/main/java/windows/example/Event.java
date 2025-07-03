package windows.example; // 声明包名，用于组织和管理类

import java.sql.Timestamp; // 导入Timestamp类，用于处理时间戳的格式化显示

/**
 * 用户点击事件类，需要满足以下Flink对POJO类的要求：
 * ⚫ 类是公有（public）的
 * ⚫ 有一个无参的构造方法
 * ⚫ 所有属性都是公有（public）的
 * ⚫ 所有属性的类型都是可以序列化的
 */
public class Event { // 定义Event类，表示用户点击事件
    public String user;     // 用户名，公有属性
    public String url;      // 访问的URL，公有属性
    public Long timestamp;  // 时间戳，表示事件发生的时间，公有属性

    public Event() {        // 无参构造方法，满足Flink对POJO类的要求
    }

    public Event(String user, String url, Long timestamp) { // 带参数的构造方法
        this.user = user;         // 初始化用户名
        this.url = url;           // 初始化URL
        this.timestamp = timestamp; // 初始化时间戳
    }

    @Override
    public String toString() {    // 重写toString方法，用于输出事件信息
        return "Event{" +
                "user='" + user + '\'' +    // 拼接用户名
                ", url='" + url + '\'' +    // 拼接URL
                ", timestamp=" + new Timestamp(timestamp) + // 将时间戳转换为可读格式
                '}';
    }
}
