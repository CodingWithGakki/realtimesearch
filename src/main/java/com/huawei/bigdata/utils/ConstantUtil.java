package com.huawei.bigdata.utils;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by ThisPC on 2019/7/11.
 */
public class ConstantUtil {
    public static final Properties PROPS = new Properties();
    public static final Logger LOG = LoggerFactory.getLogger(ConstantUtil.class);

    public static final String INPUT_PATH;

    public static final String ZK_SERVER;
    public static final String TABLE_NAME;
    public static final String COLUMN_FAMILY_1;
    public static final String COLUMN_FAMILY_2;

    public static final String INDEX_NAME;
    public static final String TYPE_NAME;

    //ES集群名,默认值elasticsearch
    public static final String CLUSTER_NAME;
    //ES集群中某个节点
    public static final String HOSTNAME;
    //ES连接端口号
    public static final int TCP_PORT;

    static {

        try {
            //加载日志配置
            PropertyConfigurator.configure(ConstantUtil.class.getClassLoader().getResource("log4j.properties").getPath());
            //加载连接配置
            PROPS.load(new FileInputStream(ConstantUtil.class.getClassLoader().getResource("conf.properties").getPath()));

        } catch (IOException e) {
            e.printStackTrace();
        }
        INPUT_PATH = PROPS.getProperty("inputPath");
        ZK_SERVER = PROPS.getProperty("ZKServer");
        TABLE_NAME = PROPS.getProperty("tableName");
        INDEX_NAME = PROPS.getProperty("indexName").toLowerCase();
        TYPE_NAME = PROPS.getProperty("typeName");
        COLUMN_FAMILY_1 = PROPS.getProperty("columnFamily1");
        COLUMN_FAMILY_2 = PROPS.getProperty("columnFamily2");
        CLUSTER_NAME = PROPS.getProperty("clusterName");
        HOSTNAME = PROPS.getProperty("hostName");
        TCP_PORT = Integer.valueOf(PROPS.getProperty("tcpPort"));

    }


}