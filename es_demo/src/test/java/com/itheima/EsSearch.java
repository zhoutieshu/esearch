package com.itheima;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;

public class EsSearch {

    private TransportClient client;

    /**
     * 查询所有 matchAllQuery
     * @throws Exception
     */
    @Test
    public void searchIndex() throws Exception{
        client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress
                        (InetAddress.getByName("127.0.0.1"), 9300));
        //设置查询条件
        SearchResponse searchResponse =
                client.prepareSearch("blog3").setTypes("article")
                        .setQuery(QueryBuilders.matchAllQuery()).get();
        //获取查询结果
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("命中数:"+totalHits);
        Iterator<SearchHit> iterator = hits.iterator();
        while(iterator.hasNext()){
            SearchHit hitFields = iterator.next();
            String sourceAsString = hitFields.getSourceAsString();
            System.out.println(sourceAsString);
            System.out.println(hitFields.getId());
            System.out.println(hitFields.getSourceAsMap().get("id"));
            System.out.println(hitFields.getSourceAsMap().get("title"));
            System.out.println(hitFields.getSourceAsMap().get("content"));
        }

        //释放资源
        client.close();
    }

    /**
     * 字符串查询-querystring
     */
    @Test
    public void searchString() throws Exception {
     searchQuery(QueryBuilders.queryStringQuery("elasticsearch"));
    }

    private void searchQuery(QueryBuilder query) throws Exception{
        //创建客户端
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new
                InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        //设置查询条件
        SearchResponse searchResponse = client.prepareSearch("blog2").setTypes("article")
                .setQuery(query).get();

        //获取查询结果
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("命中数:"+totalHits);
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()){
            SearchHit hitFields = iterator.next();
            String sourceAsString = hitFields.getSourceAsString();
            System.out.println(sourceAsString);
            System.out.println(hitFields.getId());
            System.out.println(hitFields.getSourceAsMap().get("id"));
            System.out.println(hitFields.getSourceAsMap().get("title"));
            System.out.println(hitFields.getSourceAsMap().get("content"));

        }

    }
    /**
     * 词条查询-termquery
     */
    @Test
    public void searchString1() throws Exception {
        searchQuery1(QueryBuilders.termQuery("title","搜索"));
    }

    private void searchQuery1(QueryBuilder query) throws Exception{
        //创建客户端
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new
                        InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        //设置查询条件
        SearchResponse searchResponse = client.prepareSearch("blog2").setTypes("article")
                .setQuery(query).get();

        //获取查询结果
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("命中数:"+totalHits);
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()){
            SearchHit hitFields = iterator.next();
            String sourceAsString = hitFields.getSourceAsString();
            System.out.println(sourceAsString);
            System.out.println(hitFields.getId());
            System.out.println(hitFields.getSourceAsMap().get("id"));
            System.out.println(hitFields.getSourceAsMap().get("title"));
            System.out.println(hitFields.getSourceAsMap().get("content"));

        }

    }
    @Test
    public void queryById() throws  Exception {
        // 设置搜索条件
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1","2");
        // 搜索结果
        searchQuery2(queryBuilder);
    }

    private void searchQuery2(QueryBuilder query) throws  Exception{

        //1：创建客户端
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new
                InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        //2：设置查询条件
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("content").setQuery(query).get();

        //3：获取查询结果
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("命中数:"+totalHits);
        Iterator<SearchHit> iterator = hits.iterator();
        while(iterator.hasNext()){
            SearchHit hitFields = iterator.next();
            String sourceAsString = hitFields.getSourceAsString();
            System.out.println(sourceAsString);
            System.out.println(hitFields.getId());
            System.out.println(hitFields.getSourceAsMap().get("id"));
            System.out.println(hitFields.getSourceAsMap().get("title"));
            System.out.println(hitFields.getSourceAsMap().get("content"));
        }

        //4：释放资源
        client.close();
    }
}