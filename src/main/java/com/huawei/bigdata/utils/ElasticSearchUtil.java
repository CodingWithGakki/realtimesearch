package com.huawei.bigdata.utils;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by ThisPC on 2019/7/8.
 */
public class ElasticSearchUtil {

    //构建Settings对象
    private static Settings settings = Settings.builder().put("cluster.name", ConstantUtil.CLUSTER_NAME)
            .put("client.transport.sniff", false).build();
    //TransportClient对象，用于连接ES集群
    private volatile TransportClient client;

    private final static Logger LOG = ConstantUtil.LOG;


    public ElasticSearchUtil() {
        init();

    }

    /**
     * 同步synchronized(*.class)代码块的作用和synchronized static方法作用一样,
     * 对当前对应的*.class进行持锁,static方法和.class一样都是锁的该类本身,同一个监听器
     *
     * @return
     * @throws UnknownHostException
     */
    public TransportClient getClient() {
        if (client == null) {
            synchronized (TransportClient.class) {
                try {
                    client = new PreBuiltTransportClient(settings)
                            .addTransportAddress(new TransportAddress(InetAddress.getByName(ConstantUtil.HOSTNAME), ConstantUtil.TCP_PORT));
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            }
        }
        return client;
    }

    /**
     * 获取索引管理的IndicesAdminClient
     */
    public IndicesAdminClient getAdminClient() {
        return getClient().admin().indices();
    }

    /**
     * 判定索引是否存在
     *
     * @param indexName
     * @return
     */
    public boolean isExistsIndex(String indexName) {
        IndicesExistsResponse response = getAdminClient().prepareExists(indexName).get();
        return response.isExists() ? true : false;
    }

    /**
     * 创建索引
     *
     * @param indexName
     * @return
     */
    public boolean createIndex(String indexName) {
        CreateIndexResponse createIndexResponse = getAdminClient()
                .prepareCreate(indexName.toLowerCase())
                .get();
        return createIndexResponse.isAcknowledged() ? true : false;
    }


    /**
     * 删除索引
     *
     * @param indexName
     * @return
     */
    public boolean deleteIndex(String indexName) {
        DeleteIndexResponse deleteResponse = getAdminClient()
                .prepareDelete(indexName.toLowerCase())
                .execute()
                .actionGet();
        return deleteResponse.isAcknowledged() ? true : false;
    }

    /**
     * 位索引indexName设置mapping
     *
     * @param indexName
     * @param typeName
     * @param mapping
     */
    public void setMapping(String indexName, String typeName, String mapping) {
        getAdminClient().preparePutMapping(indexName)
                .setType(typeName)
                .setSource(mapping, XContentType.JSON)
                .get();
    }

    /**
     * 创建文档,相当于往表里面insert一行数据
     *
     * @param indexName
     * @param typeName
     * @param id
     * @param document
     * @return
     * @throws IOException
     */
    public long addDocument(String indexName, String typeName, String id, Map<String, Object> document) throws IOException {
        Set<Map.Entry<String, Object>> documentSet = document.entrySet();
        IndexRequestBuilder builder = getClient().prepareIndex(indexName, typeName, id);
        XContentBuilder xContentBuilder = jsonBuilder().startObject();
        for (Map.Entry e : documentSet) {
            xContentBuilder = xContentBuilder.field(e.getKey().toString(), e.getValue());
        }
        IndexResponse response = builder.setSource(xContentBuilder.endObject()).get();
        return response.getVersion();
    }


    public List<Map<String, Object>> queryStringQuery(String text) {
        List<Map<String, Object>> resListMap = null;
        QueryBuilder match = QueryBuilders.queryStringQuery(text);
        SearchRequestBuilder search = getClient().prepareSearch()
                .setQuery(match); //分页 可选
        //搜索返回搜索结果
        SearchResponse response = search.get();
        //命中的文档
        SearchHits hits = response.getHits();
        //命中总数
        Long total = hits.getTotalHits();
        SearchHit[] hitAarr = hits.getHits();
        //循环查看命中值
        resListMap = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : hitAarr) {
            //文档元数据
            String index = hit.getIndex();
            //文档的_source的值
            Map<String, Object> resultMap = hit.getSourceAsMap();
            resListMap.add(resultMap);

        }
        return resListMap;

    }

    private void init() {
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(ConstantUtil.HOSTNAME), ConstantUtil.TCP_PORT));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }


    //用于提前建好索引，相当于关系型数据库当中的数据库
    public static void preDealCreatIndex() {
        ElasticSearchUtil esUtils = new ElasticSearchUtil();
        LOG.info("start create index..............");
        esUtils.createIndex(ConstantUtil.INDEX_NAME);
        LOG.info("finished create index !");
    }

    /**
     * 关卡登记信息bayonet：姓名，身份证号，年龄，性别，关卡号，日期时间，通关形式
     * 住宿登记信息hotel：姓名，身份证号，年龄，性别，起始日期，结束日期，同行人
     * 网吧登记信息internet：姓名，身份证号，年龄，性别，网吧名，日期，逗留时长
     * name,id,age,gender,
     * hotelAddr,hotelInTime,hotelOutTime,acquaintancer,
     * barAddr,internetDate,timeSpent,
     * bayonetAddr,crossDate,tripType
     */
    public static void preDealSetMapping() {

        JSONObject mappingTypeJson = new JSONObject();
        JSONObject propertiesJson = new JSONObject();

        JSONObject idJson = new JSONObject();
        idJson.put("type", "keyword");
        idJson.put("store", "true");
        propertiesJson.put("id", idJson);

        JSONObject nameJson = new JSONObject();
        nameJson.put("type", "keyword");
        propertiesJson.put("name", nameJson);

        JSONObject uidJson = new JSONObject();
        uidJson.put("type", "keyword");
        uidJson.put("store", "false");
        propertiesJson.put("uid", uidJson);


        JSONObject hotelAddr = new JSONObject();
        hotelAddr.put("type", "text");
        propertiesJson.put("address", hotelAddr);

        JSONObject happenedDate = new JSONObject();
        happenedDate.put("type", "date");
        happenedDate.put("format", "yyyy-MM-dd");
        propertiesJson.put("happenedDate", happenedDate);

        JSONObject endDate = new JSONObject();
        endDate.put("type", "date");
        endDate.put("format", "yyyy-MM-dd");
        propertiesJson.put("endDate", endDate);

        JSONObject acquaintancer = new JSONObject();
        acquaintancer.put("type", "keyword");
        propertiesJson.put("acquaintancer", acquaintancer);


        mappingTypeJson.put("properties", propertiesJson);

        LOG.info("start set mapping to " + ConstantUtil.INDEX_NAME + " " + ConstantUtil.TYPE_NAME + " .....");
        LOG.info(mappingTypeJson.toString());
        ElasticSearchUtil esUtils = new ElasticSearchUtil();
        esUtils.setMapping(ConstantUtil.INDEX_NAME, ConstantUtil.TYPE_NAME, mappingTypeJson.toString());
        LOG.info("set mapping done!!!");
    }

    //用于测试
    public static void test() {
        String index = "esindex";
        System.out.println("createIndex..............");
        ElasticSearchUtil esUtils = new ElasticSearchUtil();
        esUtils.createIndex(index);
        System.out.println("createIndex done!!!!!!!!!!!");
        System.out.println("isExists = " + esUtils.isExistsIndex(index));
        System.out.println("deleteIndex...............");
        esUtils.deleteIndex(index);
        System.out.println("deleteIndex done!!!!");
    }

    public static void main(String[] args) throws IOException {
        preDealCreatIndex();
        preDealSetMapping();
//        test();
    }
}