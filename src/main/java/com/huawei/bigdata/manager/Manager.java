package com.huawei.bigdata.manager;

import com.huawei.bigdata.query.Query;
import org.springframework.stereotype.Component;

@Component
public class Manager {
    private static Query query = new Query();


    public static String getQueryResult(String target) {
        try {
            String result =  query.query(target);
            System.out.println(result);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return "查询出现异常，请通知研发人员!";
        }
    }

    public static void main(String[] args) {
        String target = "牧之桃";
        String result = Manager.getQueryResult(target);
        System.out.println(result);
    }
}