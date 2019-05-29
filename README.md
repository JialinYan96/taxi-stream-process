# taxi-stream-process
读取Kafka消息，处理，存到hbase

0529 update
新建轨迹片段类 time+cell为行键  列是taxi id   keyby cell id 设立半小时时间窗口 在processwindowfunction里把半小时轨迹片段缓存着，然后变成一个segment对象，再存到hbase里
