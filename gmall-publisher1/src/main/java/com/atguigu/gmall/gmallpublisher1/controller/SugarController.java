package com.atguigu.gmall.gmallpublisher1.controller;


import com.atguigu.gmall.gmallpublisher1.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

@RestController
public class SugarController {

    @Autowired
    private ProductService productService;

    @RequestMapping("test")
    public String test1() {
        System.out.println("aaaaaa");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String nn,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

    @RequestMapping("api/sugar/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = getToday();
        }

        BigDecimal gmv = productService.getGmv(date);

        return "{ " +
                "              \"status\": 0, " +
                "              \"msg\": \"\", " +
                "              \"data\":" + gmv +
                "            }";

    }

    private int getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long ts = System.currentTimeMillis();
        return Integer.parseInt(sdf.format(ts));
    }
}