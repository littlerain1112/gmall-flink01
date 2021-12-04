package com.atguigu.gmalllogger.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;



@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("test")
    public String test() {
        System.out.println("11111");
        return "success";
    }

    @RequestMapping("test1")
    public String test1(@RequestParam("name") String name) {
        System.out.println(name);
        return "success";
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){
//        System.out.println(jsonStr);

        log.info(jsonStr);

        kafkaTemplate.send("ods_base_log",jsonStr);

        return "success";
    }

}
