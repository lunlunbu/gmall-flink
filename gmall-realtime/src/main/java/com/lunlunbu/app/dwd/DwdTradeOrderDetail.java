package com.lunlunbu.app.dwd;

import com.lunlunbu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderDetail {
    public static void main(String[] args) {

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

        //1.3设置状态的TTl,生产环境设置为最大乱序程度
        //tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        //2.读取Kafka订单预处理主题数据创建表
        tableEnv.executeSql("" +
                        "create table dwd_order_pre(\n" +
                        "    `id` string,\n" +
                        "    `order_id` string,\n" +
                        "    `sku_id` string,\n" +
                        "    `sku_name` string,\n" +
                        "    `order_price` string,\n" +
                        "    `sku_num` string,\n" +
                        "    `create_time` string,\n" +
                        "    `source_type_id` string,\n" +
                        "    `source_type_name` string,\n" +
                        "    `source_id` string,\n" +
                        "    `split_total_amount` string,\n" +
                        "    `split_activity_amount` string,\n" +
                        "    `split_coupon_amount` string,\n" +
                        "    `consignee` string,\n" +
                        "    `consignee_tel` string,\n" +
                        "    `total_amount` string,\n" +
                        "    `order_status` string,\n" +
                        "    `user_id` string,\n" +
                        "    `payment_way` string,\n" +
                        "    `delivery_address` string,\n" +
                        "    `order_comment` string,\n" +
                        "    `out_trade_no` string,\n" +
                        "    `trade_body` string,\n" +
                        "    `operate_time` string,\n" +
                        "    `expire_time` string,\n" +
                        "    `process_status` string,\n" +
                        "    `tracking_no` string,\n" +
                        "    `parent_order_id` string,\n" +
                        "    `province_id` string,\n" +
                        "    `activity_reduce_amount` string,\n" +
                        "    `coupon_reduce_amount` string,\n" +
                        "    `original_total_amount` string,\n" +
                        "    `feight_fee` string,\n" +
                        "    `feight_fee_reduce` string,\n" +
                        "    `refundable_time` string,\n" +
                        "    `order_detail_activity_id` string,\n" +
                        "    `activity_id` string,\n" +
                        "    `activity_rule_id` string,\n" +
                        "    `order_detail_coupon_id` string,\n" +
                        "    `coupon_id` string,\n" +
                        "    `coupon_use_id` string,\n" +
                        "    `type` string,\n" +
                        "    `old` map<string,string>,\n" +
                        "    `row_op_ts` TIMESTAMP_LTZ(3)\n" +
                        ") " + MyKafkaUtil.getKafkaDDL("dwd_trade_order_pre_process", "order_detail_211126"));

        //3.过滤出下单数据，即新增数据
        Table filteredTable = tableEnv.sqlQuery("" +
                "select " +
                "id,\n" +
                "order_id,\n" +
                "user_id,\n" +
                "sku_id,\n" +
                "sku_name,\n" +
                "sku_num,\n" +
                "order_price,\n" +
                "province_id,\n" +
                "activity_id,\n" +
                "activity_rule_id,\n" +
                "coupon_id,\n" +
                //"date_id,\n" +
                "create_time,\n" +
                "source_id,\n" +
                "source_type_id,\n" +
                "source_type_name,\n" +
                //"sku_num,\n" +
                //"split_original_amount,\n" +
                "split_activity_amount,\n" +
                "split_coupon_amount,\n" +
                "split_total_amount, \n" +
                //"od_ts ts,\n" +
                "row_op_ts\n" +
                "from dwd_order_pre " +
                "where `type`='insert'");
        tableEnv.createTemporaryView("filtered_table", filteredTable);

        //4.创建DWD层下单数据表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "sku_num string,\n" +
                "order_price string,\n" +
                "province_id string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                //"date_id string,\n" +
                "create_time string,\n" +
                "source_id string,\n" +
                "source_type_id string,\n" +
                "source_type_name string,\n" +
                //"sku_num string,\n" +
                //"split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string, \n" +
                //"ts string,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_order_detail"));


        //5.将数据写出到Kafka
        tableEnv.executeSql("insert into dwd_trade_order_detail select * from filtered_table");

        //6.启动任务

    }
}
