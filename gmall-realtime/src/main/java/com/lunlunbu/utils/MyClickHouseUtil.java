package com.lunlunbu.utils;

import com.lunlunbu.bean.TransientSink;
import com.lunlunbu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {

    public static <T> SinkFunction<T> getSinkFunction(String sql){

        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //使用反射得方法获取t对象中得数据
                        Class<?> tClz = t.getClass();

                        //获取并遍历属性
                        Field[] declaredFields = tClz.getDeclaredFields();
                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {

                            //获取单个属性
                            Field field = declaredFields[i];
                            field.setAccessible(true);

                            //尝试获取字段上的自定义注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null){
                                offset++;
                                continue;
                            }

                            //获取属性值
                            Object value = field.get(t);

                            //给占位符赋值
                            preparedStatement.setObject(i + 1 - offset, value);

                        }

                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }

}
