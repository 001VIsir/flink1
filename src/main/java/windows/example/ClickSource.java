package windows.example; // 声明包名，用于组织和管理类

import org.apache.flink.streaming.api.functions.source.SourceFunction; // 导入Flink的数据源接口

import java.util.Calendar; // 导入Calendar类，用于获取当前系统时间
import java.util.Random;   // 导入Random类，用于生成随机数

public class ClickSource implements SourceFunction<Event> { // 实现SourceFunction接口，泛型参数为Event类型

    private Boolean running = true; // 控制数据生成循环的标志，初始值为true表示开始生成数据

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception { // 实现run方法，定义数据生成逻辑
        Random random = new Random(); // 创建随机数生成器
        String[] users = {"jt", "ahao", "sunrui"}; // 定义用户名数组，用于随机选择用户
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"}; // 定义URL数组，用于随机选择URL

        while (running) { // 当running为true时，持续生成数据
            sourceContext.collect(new Event( // 通过sourceContext的collect方法发出一个新的事件
                    users[random.nextInt(users.length)], // 随机选择一个用户名
                    urls[random.nextInt(urls.length)],   // 随机选择一个URL
                    Calendar.getInstance().getTimeInMillis() // 获取当前系统时间的毫秒值作为时间戳
            ));
            Thread.sleep(1000); // 线程休眠1秒，控制数据生成速率
        }
    }

    @Override
    public void cancel() { // 实现cancel方法，用于停止数据生成
        running = false;   // 将running设置为false，使run方法中的循环退出
    }
}
