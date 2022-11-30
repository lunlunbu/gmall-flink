package com.atguigu.app.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordBean;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        //1.1开启CheckPoint
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//
//        //1.2设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/211126/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.使用DDL读取Kafka page_log 主题数据创建表并提取时间戳生成Watermark
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";

        tableEnv.executeSql("" +
                "create table page_log(\n" +
                "    `page` map<string,string>,\n" +
                "    `ts` bigint,\n" +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),\n" +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND\n" +
                ")" + MyKafkaUtil.getKafkaDDL(topic, groupId));

        //TODO 3.过滤出搜索数据
        Table filterTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    page['item'] item,\n" +
                "    rt\n" +
                "from page_log\n" +
                "where page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'\n" +
                "and page['item'] is not null   filter_table");
        tableEnv.createTemporaryView("filter_table", filterTable);

        //TODO 4.注册UDTF & 切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT\n" +
                "    word,\n" +
                "    rt\n" +
                "FROM filter_table, \n" +
                "LATERAL TABLE(SplitFunction(item))");
        tableEnv.createTemporaryView("split_table", splitTable);

        //TODO 5.分组，开窗，聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "    'search' source,\n" +
                "    word keyword,\n" +
                "    count(*) keyword_count,\n" +
                "    UNIX_TIMESTAMP()*1000 ts\n" +
                "from split_table\n" +
                "group by word,TUMBLE(rt, INTERVAL '10' SECOND)");

        //TODO 6.将动态表转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print(">>>>>>>>>>");

        //TODO 7.将数据写出到ClickHouse
        keywordBeanDataStream.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("DwsTrafficSourceKeywordPageViewWindow");

    }

}
