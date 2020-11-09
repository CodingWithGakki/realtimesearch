package com.huawei.bigdata.controller;

import org.springframework.boot.SpringApplication;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Created by ThisPC on 2019/7/14.
 * <p>
 * 注解声明，该类为Controller类 并自动加载所需要的其它类
 */
@Controller
public class SearchController {
    @RequestMapping("/search_target")
    String testdo(ModelMap map) {
//这里返回HTML页面
        return "info_target_search";
    }

    // 主方法，像一般的Java类一般去右击run as application时候，执行该方法
    public static void main(String[] args) {
        SpringApplication.run(SearchController.class, args);
    }

}