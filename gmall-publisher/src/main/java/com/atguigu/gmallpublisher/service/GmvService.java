package com.atguigu.gmallpublisher.service;

import java.math.BigDecimal;
import java.util.Map;

public interface GmvService {

    //获取GMV总数
    Double getGmv(int date);

    //根据Tm获取订单总金额
    Map getGmvByTm(int date, int limit);
}
