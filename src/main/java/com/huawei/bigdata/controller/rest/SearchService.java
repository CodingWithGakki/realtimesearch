package com.huawei.bigdata.controller.rest;

import com.huawei.bigdata.manager.Manager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by ThisPC on 2019/7/10.
 */
@RestController
@EnableAutoConfiguration
@ComponentScan(basePackages = {"com.huawei.bigdata"})
public class SearchService {
    @RequestMapping("/search")
    public String search(String target) {
        try {
            return Manager.getQueryResult(target);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "不小心出错了!";
    }

    // 主方法，像一般的Java类一般去右击run as application时候，执行该方法
    public static void main(String[] args) throws Exception {
        SpringApplication.run(SearchService.class, args);
    }
}