package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Locale;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private static DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建连接池
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获得链接
        DruidPooledConnection connection = druidDataSource.getConnection();

        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");

        //获取数据类型
        String type = value.getString("type");
        //如果是更新数据，则需要删除redis中的数据
        DimUtil.delDimInfo(sinkTable.toUpperCase(), data.getString("id"));

        //写出数据
        PhoenixUtil.insertValues(connection, sinkTable, data);

        //归还连接
        connection.close();

    }
}
