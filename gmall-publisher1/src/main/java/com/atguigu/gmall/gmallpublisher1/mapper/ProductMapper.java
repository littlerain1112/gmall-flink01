package com.atguigu.gmall.gmallpublisher1.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

public interface ProductMapper {

    @Select("select sum(order_amount) from product_stats_210625 where toYYYYMMDD(stt)=${date}")
    BigDecimal selectGmv(int date);

}
