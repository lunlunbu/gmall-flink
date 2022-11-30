package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwdTradeCartAdd {

    public static void main(String[] args) throws Exception {
        //1.获取执行环境
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

        //2.使用DDL方式读取topic_db主题的数据建表
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("cart_add_211126"));

        //3.过滤出加购数据
        Table cartAddTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['cart_price'] cart_price,\n" +
                "    if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['is_checked'] is_checked,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['operate_time'] operate_time,\n" +
                "    `data`['is_ordered'] is_ordered,\n" +
                "    `data`['order_time'] order_time,\n" +
                "    `data`['source_type'] source_type,\n" +
                "    `data`['source_id'] source_id,\n" +
                "    pt\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'cart_info'\n" +
                "and `type` = 'insert'\n" +
                "or (`type` = 'update' \n" +
                "    and \n" +
                "    `old`['sku_num'] is not null \n" +
                "    and \n" +
                "    cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int))"
        );

        //将加购表转换为流并打印测试
        tableEnv.createTemporaryView("cart_info_table", cartAddTable);
        //tableEnv.toAppendStream(cartAddTable, Row.class).print(">>>>>>>");

        //4.读取MySQL的base_dic表作为LookUp表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //5.关联两张表
        Table cartAddWithDicTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    ci.id,\n" +
                "    ci.user_id,\n" +
                "    ci.sku_id,\n" +
                "    ci.cart_price,\n" +
                "    ci.sku_num,\n" +
                "    ci.sku_name,\n" +
                "    ci.is_checked,\n" +
                "    ci.create_time,\n" +
                "    ci.operate_time,\n" +
                "    ci.is_ordered,\n" +
                "    ci.order_time,\n" +
                "    ci.source_type source_type_id,\n" +
                "    dic.dic_name source_type_name,\n" +
                "    ci.source_id\n" +
                "from cart_info_table ci\n" +
                "join base_dic FOR SYSTEM_TIME AS OF ci.pt as dic\n" +
                "on ci.source_type = dic.dic_code\n");

        //6.使用DDL方式创建加购事实表
        tableEnv.executeSql("" +
                "create table dwd_cart_add(\n" +
                "    `id` STRING,\n" +
                "    `user_id` STRING,\n" +
                "    `sku_id` STRING,\n" +
                "    `cart_price` STRING,\n" +
                "    `sku_num` STRING,\n" +
                "    `sku_name` STRING,\n" +
                "    `is_checked` STRING,\n" +
                "    `create_time` STRING,\n" +
                "    `operate_time` STRING,\n" +
                "    `is_ordered` STRING,\n" +
                "    `order_time` STRING,\n" +
                "    `source_type_id` STRING,\n" +
                "    `source_type_name` STRING,\n" +
                "    `source_id` STRING\n" +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));

        //7.将数据写出
        tableEnv.executeSql("insert into dwd_cart_add select * from " + cartAddWithDicTable)
                .print();

        //8.启动任务
        env.execute("DwdTradeCartAdd");
    }

}
