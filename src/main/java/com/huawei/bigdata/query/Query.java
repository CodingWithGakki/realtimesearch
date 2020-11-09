package com.huawei.bigdata.query;

import com.alibaba.fastjson.JSONObject;
import com.huawei.bigdata.utils.ConstantUtil;
import com.huawei.bigdata.utils.ElasticSearchUtil;
import com.huawei.bigdata.utils.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by ThisPC on 2019/7/9.
 * 搜索逻辑是先搜索ElasticSearch，再查HBase
 */
public class Query {
    private HBaseUtil hBaseUtil = new HBaseUtil();
    private ElasticSearchUtil elasticSearchUtil = new ElasticSearchUtil();
    private JSONObject result = new JSONObject();
    private JSONObject tmpJS = new JSONObject();

    public String query(String target) {
        result.clear();
        tmpJS.clear();

        long startTime = System.currentTimeMillis();
        List<Map<String, Object>> listMap = elasticSearchUtil.queryStringQuery(target);
        long endTime = System.currentTimeMillis();
        float seconds = (endTime - startTime) / 1000F;
        ConstantUtil.LOG.info("ElasticSearch查询耗时" + Float.toString(seconds) + " seconds.");
        for (Map<String, Object> m : listMap) {
            String id = m.get("id").toString();
            JSONObject tmpJS = new JSONObject();
            tmpJS.put("id", id);
            Result res = null;
            try {
                long s1 = System.currentTimeMillis();
                res = hBaseUtil.get(ConstantUtil.TABLE_NAME, id);
                long e1 = System.currentTimeMillis();
                float se1 = (e1 - s1) / 1000F;
                ConstantUtil.LOG.info("HBase查询耗时" + Float.toString(se1) + " seconds.");
                Cell[] cells = res.rawCells();
                for (Cell cell : cells) {
                    String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                    System.out.println(col);
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(value);
                    tmpJS.put(col, value);
                }
                result.put(id, tmpJS);
            } catch (IOException e) {
                e.printStackTrace();
                result.put(id, "查询失败!");
            }
        }
        return result.toString();
    }

    public static void main(String[] args) throws Exception {
        Query query = new Query();
        long startTime = System.currentTimeMillis();
        System.out.println(query.query("100004"));
        long endTime = System.currentTimeMillis();
        float seconds = (endTime - startTime) / 1000F;
        ConstantUtil.LOG.info("  耗时" + Float.toString(seconds) + " seconds.");
    }
}
