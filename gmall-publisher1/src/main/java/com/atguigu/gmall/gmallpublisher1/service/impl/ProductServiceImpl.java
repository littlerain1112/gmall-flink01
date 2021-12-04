package com.atguigu.gmall.gmallpublisher1.service.impl;

import com.atguigu.gmall.gmallpublisher1.mapper.ProductMapper;
import com.atguigu.gmall.gmallpublisher1.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class ProductServiceImpl implements ProductService {

    @Override
    public BigDecimal getGmv(int date) {
        return productMapper.selectGmv(date);
    }

    @Autowired
    private ProductMapper productMapper;

}
