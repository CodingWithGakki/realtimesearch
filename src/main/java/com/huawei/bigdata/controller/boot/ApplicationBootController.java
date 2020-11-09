package com.huawei.bigdata.controller.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * 根启动类
 * Created by ThisPC on 2019/7/14.
 */
@SpringBootApplication
@ComponentScan(basePackages = "com.huawei.bigdata")
public class ApplicationBootController {
    public static void main(String[] args) {
        SpringApplication.run(ApplicationBootController.class, args);
    }
}