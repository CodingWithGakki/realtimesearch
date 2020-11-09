package com.huawei.bigdata.insert;

import com.huawei.bigdata.utils.ConstantUtil;
import com.huawei.bigdata.utils.ElasticSearchUtil;
import com.huawei.bigdata.utils.HBaseUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

/**
 * Created by ThisPC on 2019/7/8.
 */

/**
 * 读取本地文件并解析数据，之后插入HBase、ElasticSearch和graphBase 。
 * 对应华为云服务为CloudTable、CSS、GES
 */
public class LoadData2HBaseAndElasticSearch {

    private HBaseUtil hBaseUtil;
    private ElasticSearchUtil elasticSearchUtil;


    public LoadData2HBaseAndElasticSearch() {
    }


    /**
     * 关卡登记信息bayonet：姓名，身份证号，年龄，性别，关卡号，日期时间，通关形式
     * 住宿登记信息hotel：姓名，身份证号，年龄，性别，起始日期，结束日期，同行人
     * 网吧登记信息internet：姓名，身份证号，年龄，性别，网吧名，日期，逗留时长
     * name,uid,age,gender,
     * hotelAddr,happenedDate,endDate,acquaintancer,
     * barAddr,happenedDate,duration,
     * bayonetAddr,happenedDate,tripType
     */
    public void insert() throws Exception {
        hBaseUtil = new HBaseUtil();
        elasticSearchUtil = new ElasticSearchUtil();
        String filePath = ConstantUtil.INPUT_PATH;

        File dir = new File(filePath);
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    System.out.println(file.getName() + "This is a directory!");
                } else {
                    //住宿登记信息
                    if (file.getName().contains("hotel")) {
                        BufferedReader reader = null;
                        reader = new BufferedReader(new FileReader(filePath + file.getName()));
                        String tempString = null;
                        while ((tempString = reader.readLine()) != null) {
                            //Blank line judgment
                            if (!tempString.isEmpty()) {
                                List<Put> putList = new ArrayList<Put>();
                                String[] elements = tempString.split(",");
                                //生成不重复用户ID，
                                String id = UUID.randomUUID().toString();
                                Put put = new Put(Bytes.toBytes(id));
                                //将数据添加至hbase库
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("name"), Bytes.toBytes(elements[0]));
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("uid"), Bytes.toBytes(elements[1]));
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("age"), Bytes.toBytes(elements[2]));
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("gender"), Bytes.toBytes(elements[3]));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("event"), Bytes.toBytes("hotel"));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("address"), Bytes.toBytes(elements[4]));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("happenedDate"), Bytes.toBytes(elements[5]));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("endDate"), Bytes.toBytes(elements[6]));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("acquaintancer"), Bytes.toBytes(elements[7]));
                                putList.add(put);
                                ConstantUtil.LOG.info("hotel_info start putting to HBase ....:" + id + " " + tempString);
                                hBaseUtil.put(ConstantUtil.TABLE_NAME, putList);
                                //将数据添加至ES库
                                Map<String, Object> esMap = new HashMap<String, Object>();
                                esMap.put("id", id);
                                esMap.put("name", elements[0]);
                                esMap.put("uid", elements[1]);
                                esMap.put("address", elements[4]);
                                esMap.put("happenedDate", elements[5]);
                                esMap.put("endDate", elements[6]);
                                esMap.put("acquaintancer", elements[7]);
                                elasticSearchUtil.addDocument(ConstantUtil.INDEX_NAME, ConstantUtil.TYPE_NAME, id, esMap);
                                ConstantUtil.LOG.info("start add document to ES..." + ConstantUtil.INDEX_NAME + " " + ConstantUtil.TYPE_NAME + " " + id + " " + esMap);
                            }
                        }

                        reader.close();
                    }
                    //网吧登记信息
                    else if (file.getName().contains("internet")) {
                        BufferedReader reader = null;
                        reader = new BufferedReader(new FileReader(filePath + file.getName()));
                        String tempString = null;
                        while ((tempString = reader.readLine()) != null) {
                            //Blank line judgment
                            if (!tempString.isEmpty()) {
                                List<Put> putList = new ArrayList<Put>();
                                String[] elements = tempString.split(",");
                                //生成不重复用户ID，
                                String id = UUID.randomUUID().toString();
                                Put put = new Put(Bytes.toBytes(id));
                                //将数据添加至hbase库
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("name"), Bytes.toBytes(elements[0]));
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("uid"), Bytes.toBytes(elements[1]));
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("age"), Bytes.toBytes(elements[2]));
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("gender"), Bytes.toBytes(elements[3]));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("event"), Bytes.toBytes("internetBar"));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("address"), Bytes.toBytes(elements[4]));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("happenedDate"), Bytes.toBytes(elements[5]));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("duration"), Bytes.toBytes(elements[6]));
                                putList.add(put);
                                ConstantUtil.LOG.info("internet_info start putting to HBase ... :" + id + " " + tempString);
                                hBaseUtil.put(ConstantUtil.TABLE_NAME, putList);
                                //将数据添加至ES库
                                Map<String, Object> esMap = new HashMap<String, Object>();
                                esMap.put("id", id);
                                esMap.put("name", elements[0]);
                                esMap.put("uid", elements[1]);
                                esMap.put("address", elements[4]);
                                esMap.put("happenedDate", elements[5]);
                                elasticSearchUtil.addDocument(ConstantUtil.INDEX_NAME, ConstantUtil.TYPE_NAME, id, esMap);
                                ConstantUtil.LOG.info("start add document to ES..." + ConstantUtil.INDEX_NAME + " " + ConstantUtil.TYPE_NAME + " " + id + " " + esMap);


                            }
                        }
                        reader.close();

                    }
                    //关卡登记信息
                    else if (file.getName().contains("bayonet")) {
                        BufferedReader reader = null;
                        reader = new BufferedReader(new FileReader(filePath + file.getName()));
                        String tempString = null;
                        while ((tempString = reader.readLine()) != null) {
                            //Blank line judgment
                            if (!tempString.isEmpty()) {
                                List<Put> putList = new ArrayList<Put>();
                                String[] elements = tempString.split(",");
                                //生成不重复用户ID，
                                String id = UUID.randomUUID().toString();
                                Put put = new Put(Bytes.toBytes(id));
                                //将数据添加至hbase库
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("name"), Bytes.toBytes(elements[0]));
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("uid"), Bytes.toBytes(elements[1]));
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("age"), Bytes.toBytes(elements[2]));
                                put.addColumn(Bytes.toBytes("Basic"), Bytes.toBytes("gender"), Bytes.toBytes(elements[3]));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("event"), Bytes.toBytes("bayonet"));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("address"), Bytes.toBytes(elements[4]));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("happenedDate"), Bytes.toBytes(elements[5]));
                                put.addColumn(Bytes.toBytes("OtherInfo"), Bytes.toBytes("tripType"), Bytes.toBytes(elements[6]));
                                putList.add(put);
                                hBaseUtil.put(ConstantUtil.TABLE_NAME, putList);
                                ConstantUtil.LOG.info("bayonet_info start putting to HBase....:" + id + " " + tempString);
                                //将数据添加至ES库
                                Map<String, Object> esMap = new HashMap<String, Object>();
                                esMap.put("id", id);
                                esMap.put("name", elements[0]);
                                esMap.put("uid", elements[1]);
                                esMap.put("address", elements[4]);
                                esMap.put("happenedDate", elements[5]);
                                elasticSearchUtil.addDocument(ConstantUtil.INDEX_NAME, ConstantUtil.TYPE_NAME, id, esMap);
                                ConstantUtil.LOG.info("start add document to ES..." + ConstantUtil.INDEX_NAME + " " + ConstantUtil.TYPE_NAME + " " + id + " " + esMap);
                            }
                        }
                        reader.close();
                    }
                    //数据描述文件跳过
                    else {
                        continue;
                    }
                }
            }
            ConstantUtil.LOG.info("load and insert done !!!!!!!!!!!!!!!!!!");
        }

    }

    public static void start() throws Exception {
        LoadData2HBaseAndElasticSearch load2DB = new LoadData2HBaseAndElasticSearch();
        load2DB.insert();
    }


    public static void main(String[] args) throws Exception {
        start();
    }
}