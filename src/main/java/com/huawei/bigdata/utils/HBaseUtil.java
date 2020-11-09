package com.huawei.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ThisPC on 2019/7/8.
 */
public class HBaseUtil {

    /**
     * HBase连接的基本配置
     */
    public static Admin admin = null;
    public static Configuration conf = null;
    public static Connection conn = null;
    private HashMap<String, Table> tables = null;
    private static final Logger LOG = ConstantUtil.LOG;


    /**
     * 构造函数加载配置
     */
    public HBaseUtil() {
        this(ConstantUtil.ZK_SERVER);
    }

    public HBaseUtil(String zkServer) {
        init(zkServer);
    }


    private void ifNotConnTableJustConn(String tableName) {
        if (!tables.containsKey(tableName)) {
            this.addTable(tableName);
        }
    }

    public Table getTable(String tableName) {
        ifNotConnTableJustConn(tableName);
        return tables.get(tableName);
    }

    public void addTable(String tableName) {
        try {
            tables.put(tableName, conn.getTable(TableName.valueOf(tableName)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过 LIst<put> 直接插入一批数据
     *
     * @param putList
     * @return
     */
    public boolean put(String tableName, List<Put> putList) throws Exception {
        boolean res = false;
        ifNotConnTableJustConn(tableName);
        try {
            getTable(tableName).put(putList);
            res = true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return res;

    }

    /**
     * 读取一行记录，一个rowKey的所有记录
     *
     * @param tableName
     * @param row
     * @return
     * @throws IOException
     */
    public Result get(String tableName, String row) throws IOException {
        Result result = null;
        ifNotConnTableJustConn(tableName);
        Table newTable = getTable(tableName);
        Get get = new Get(Bytes.toBytes(row));
        try {
            result = newTable.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    /**
     * 创建表
     *
     * @param tableName
     * @param columnFamilys
     */


    public boolean createTable(String tableName, String... columnFamilys) {
        boolean result = false;
        try {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                LOG.info(tableName + "表已经存在！");
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
                for (String columnFamily : columnFamilys) {
                    tableDesc.addFamily(new HColumnDescriptor(columnFamily.getBytes()));
                }

                admin.createTable(tableDesc);
                result = true;
                LOG.info(tableName + "表创建成功！");
            }

        } catch (IOException e) {
            e.printStackTrace();
            LOG.info(tableName + "表创建失败 ！");
        }
        return result;
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     */
    public boolean tableExists(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 停用表
     *
     * @param tableName
     */
    public void disableTable(String tableName) throws IOException {
        if (tableExists(tableName)) {
            admin.disableTable(TableName.valueOf(tableName));
        }

    }

    /**
     * 删除表
     *
     * @param tableName
     */
    public void deleteTable(String tableName) throws IOException {
        disableTable(tableName);
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 查询所有表名
     *
     * @return
     * @throws Exception
     */
    public List<String> getALLTableName() throws Exception {
        ArrayList<String> tableNames = new ArrayList<String>();
        if (admin != null) {
            HTableDescriptor[] listTables = admin.listTables();
            if (listTables.length > 0) {
                for (HTableDescriptor tableDesc : listTables) {
                    tableNames.add(tableDesc.getNameAsString());
                }
            }
        }
        return tableNames;
    }

    /**
     * 删除所有表,慎用!仅用于测试环境
     */
    public void deleteAllTable() throws Exception {
        List<String> allTbName = getALLTableName();
        for (String s : allTbName) {
            LOG.info("Start delete table : " + s + "......");
            deleteTable(s);
            LOG.info("done delete table : " + s);
        }
    }

    /**
     * 初始化配置
     *
     * @param zkServer
     */

    public void init(String zkServer) {
        tables = new HashMap<String, Table>();
        conf = HBaseConfiguration.create();

        //通过CSS  cloudTable服务列表获取的ZK连接地址
        //cloudtable-f7c2-zk1-nMuTH9Xv.cloudtable.com:2181,cloudtable-f7c2-zk2-5z92kpre.cloudtable.com:2181,cloudtable-f7c2-zk3-xVNq61Sb.cloudtable.com:2181
        //192.168.0.121:2181  运行后可看到日志打印具体内网地址
        conf.set("hbase.zookeeper.quorum", zkServer);
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 清理所有连接
     *
     * @throws IOException
     */
    public void clear() throws IOException {
        for (Map.Entry<String, Table> m : tables.entrySet()) {
            m.getValue().close();
        }
        admin.close();
        conn.close();
        conf.clear();
    }

    /**
     * 关卡登记信息bayonet：姓名，身份证号，年龄，性别，关卡号，日期时间，通关形式
     * 住宿登记信息hotel：姓名，身份证号，年龄，性别，起始日期，结束日期，同行人
     * 网吧登记信息internet：姓名，身份证号，年龄，性别，网吧名，日期，逗留时长
     */
    //用于提前建好表和列族
    public static void preDeal() throws Exception {
        HBaseUtil hBaseUtils = new HBaseUtil();
        hBaseUtils.createTable(ConstantUtil.TABLE_NAME, ConstantUtil.COLUMN_FAMILY_1, ConstantUtil.COLUMN_FAMILY_2);

    }

    //测试
    public static void test() throws Exception {
        HBaseUtil hBaseUtils = new HBaseUtil();
        long startTime = System.currentTimeMillis();
        String tb = "testTb";
        String colFamily = "info";
        String col = "name";
        String row = "100000";
        String value = "张三";
        hBaseUtils.createTable(tb, colFamily);
        List<Put> listPut = new ArrayList<>();
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
        listPut.add(put);
        hBaseUtils.put(tb, listPut);
        Result res = hBaseUtils.get("testTb", "100000");
        List<Cell> list = res.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("name"));
        for (Cell c : list) {
            LOG.info(Bytes.toString(CellUtil.cloneFamily(c)));
            LOG.info(Bytes.toString(CellUtil.cloneQualifier(c)));
            LOG.info(Bytes.toString(CellUtil.cloneValue(c)));
        }

        long endTime = System.currentTimeMillis();
        float seconds = (endTime - startTime) / 1000F;
        LOG.info("  耗时" + Float.toString(seconds) + " seconds.");
    }


    public static void main(String[] args) throws Exception {
//        test();
        preDeal();
    }

}