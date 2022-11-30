package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;


public class DwdTradeOrderPreProcess {

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

        //1.3设置状态的TTl,生产环境设置为最大乱序程度
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        //2.创建topic_db表
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("order_pre_process"));

        //3.过滤出订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    data['id'] id,\n" +
                "    data['order_id'] order_id,\n" +
                "    data['sku_id'] sku_id,\n" +
                "    data['sku_name'] sku_name,\n" +
                "    data['order_price'] order_price,\n" +
                "    data['sku_num'] sku_num,\n" +
                "    data['create_time'] create_time,\n" +
                "    data['source_type'] source_type,\n" +
                "    data['source_id'] source_id,\n" +
                "    data['split_total_amount'] split_total_amount,\n" +
                "    data['split_activity_amount'] split_activity_amount,\n" +
                "    data['split_coupon_amount'] split_coupon_amount,\n" +
                "    pt \n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail' \n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_table", orderDetailTable);

        //转换为流进行打印测试
        tableEnv.toAppendStream(orderDetailTable, Row.class).print();

        //4.过滤出订单数据
        Table orderInfoTable = tableEnv.sqlQuery("" +
                "\n" +
                "select\n" +
                "    data['id'] id,\n" +
                "    data['consignee'] consignee,\n" +
                "    data['consignee_tel'] consignee_tel,\n" +
                "    data['total_amount'] total_amount,\n" +
                "    data['order_status'] order_status,\n" +
                "    data['user_id'] user_id,\n" +
                "    data['payment_way'] payment_way,\n" +
                "    data['delivery_address'] delivery_address,\n" +
                "    data['order_comment'] order_comment,\n" +
                "    data['out_trade_no'] out_trade_no,\n" +
                "    data['trade_body'] trade_body,\n" +
                "    data['create_time'] create_time,\n" +
                "    data['operate_time'] operate_time,\n" +
                "    data['expire_time'] expire_time,\n" +
                "    data['process_status'] process_status,\n" +
                "    data['tracking_no'] tracking_no,\n" +
                "    data['parent_order_id'] parent_order_id,\n" +
                "    data['province_id'] province_id,\n" +
                "    data['activity_reduce_amount'] activity_reduce_amount,\n" +
                "    data['coupon_reduce_amount'] coupon_reduce_amount,\n" +
                "    data['original_total_amount'] original_total_amount,\n" +
                "    data['feight_fee'] feight_fee,\n" +
                "    data['feight_fee_reduce'] feight_fee_reduce,\n" +
                "    data['refundable_time'] refundable_time,\n" +
                "    `type`,\n" +
                "    `old`\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_info' \n" +
                "and (`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("order_info_table", orderInfoTable);

        //转换为流并打印测试
        tableEnv.toAppendStream(orderInfoTable, Row.class).print(">>>>>>>>");

        //5.过滤出订单明细活动关联数据
        Table orderActivityTable = tableEnv.sqlQuery("" +
                "\n" +
                "select\n" +
                "    data['id'] id,\n" +
                "    data['order_id'] order_id,\n" +
                "    data['order_detail_id'] order_detail_id,\n" +
                "    data['activity_id'] activity_id,\n" +
                "    data['activity_rule_id'] activity_rule_id,\n" +
                "    data['sku_id'] sku_id,\n" +
                "    data['create_time'] create_time\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail_activity' \n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_activity_table", orderActivityTable);


        tableEnv.toAppendStream(orderActivityTable, Row.class).print(">>>>>>>");

        //6.过滤出订单明细购物券关联数据
        Table orderCouponTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['order_id'] order_id, " +
                "    data['order_detail_id'] order_detail_id, " +
                "    data['coupon_id'] coupon_id, " +
                "    data['coupon_use_id'] coupon_use_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall-211126-flink' " +
                "and `table` = 'order_detail_coupon' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_coupon_table", orderCouponTable);

        tableEnv.toAppendStream(orderCouponTable, Row.class).print(">>>>>>>>");


        //7.创建base_dic LookUp表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //8. 关联5张表
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    od.id,\n" +
                "    od.order_id,\n" +
                "    od.sku_id,\n" +
                "    od.sku_name,\n" +
                "    od.order_price,\n" +
                "    od.sku_num,\n" +
                "    od.create_time,\n" +
                "    od.source_type source_type_id,\n" +
                "    dic.dic_name source_type_name,\n" +
                "    od.source_id,\n" +
                "    od.split_total_amount,\n" +
                "    od.split_activity_amount,\n" +
                "    od.split_coupon_amount,\n" +
                "    oi.consignee,\n" +
                "    oi.consignee_tel,\n" +
                "    oi.total_amount,\n" +
                "    oi.order_status,\n" +
                "    oi.user_id,\n" +
                "    oi.payment_way,\n" +
                "    oi.delivery_address,\n" +
                "    oi.order_comment,\n" +
                "    oi.out_trade_no,\n" +
                "    oi.trade_body,\n" +
                "    oi.operate_time,\n" +
                "    oi.expire_time,\n" +
                "    oi.process_status,\n" +
                "    oi.tracking_no,\n" +
                "    oi.parent_order_id,\n" +
                "    oi.province_id,\n" +
                "    oi.activity_reduce_amount,\n" +
                "    oi.coupon_reduce_amount,\n" +
                "    oi.original_total_amount,\n" +
                "    oi.feight_fee,\n" +
                "    oi.feight_fee_reduce,\n" +
                "    oi.refundable_time,\n" +
                "    oa.id order_detail_activity_id,\n" +
                "    oa.activity_id,\n" +
                "    oa.activity_rule_id,\n" +
                "    oc.id order_detail_coupon_id,\n" +
                "    oc.coupon_id,\n" +
                "    oc.coupon_use_id,\n" +
                "    oi.`type`,\n" +
                "    oi.`old`,\n" +
                "    current_row_timestamp() row_op_ts " +
                "from order_detail_table od\n" +
                "join order_info_table oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_activity_table oa\n" +
                "on od.id = oa.order_detail_id\n" +
                "left join order_coupon_table oc\n" +
                "on od.id = oc.order_detail_id\n" +
                "join base_dic FOR SYSTEM_TIME AS OF od.pt as dic\n" +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        tableEnv.toRetractStream(resultTable, Row.class).print(">>>>>>>>");

        //9.创建upsert-kafka表
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
                "     row_op_ts TIMESTAMP_LTZ(3), " +
                "     primary key(id) not enforced\n" +
                ") " + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));

        //10.将数据写出
        tableEnv.executeSql("insert into dwd_order_pre select * from result_table");

        //11.启动任务
        env.execute();

    }

}
