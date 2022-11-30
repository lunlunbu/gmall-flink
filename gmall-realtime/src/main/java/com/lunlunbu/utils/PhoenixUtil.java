package com.lunlunbu.utils;


import com.alibaba.fastjson.JSONObject;
import com.lunlunbu.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {
    /**
     * Phoenix 表数据导入方法
     *
     * @param conn 连接对象
     * @param sinkTable 写入数据的 Phoenix 目标表名
     * @param data      待写入的数据
     */
    public static void insertValues(Connection conn, String sinkTable, JSONObject data) {
        // 获取字段名
        Set<String> columns = data.keySet();
        // 获取字段对应的值
        Collection<Object> values = data.values();
        // 拼接字段名
        String columnStr = StringUtils.join(columns, ",");
        // 拼接字段值
        String valueStr = StringUtils.join(values, "','");
        // 拼接插入语句
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(" +
                columnStr + ") values ('" + valueStr + "')";

        // 为数据库操作对象赋默认值
        PreparedStatement preparedSt = null;

        // 执行 SQL
        try {
            preparedSt = conn.prepareStatement(sql);
            preparedSt.execute();
            // 提交事务
            conn.commit();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            throw new RuntimeException("数据库操作对象获取或执行异常");
        } finally {
            if (preparedSt != null) {
                try {
                    preparedSt.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                    throw new RuntimeException("数据库操作对象释放异常");
                }
            }
        }
    }
}

