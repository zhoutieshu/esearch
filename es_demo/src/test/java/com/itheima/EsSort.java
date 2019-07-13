package com.itheima;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.itheima.domain.Content;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;

public class EsSort {

    @Test
    public void batchIndex() throws  Exception{
        //创建客户端
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new
                InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        ObjectMapper objectMapper=new ObjectMapper();
        //批量插入数据
        for(int i=1;i<=50;i++){
            Content content= new Content();
            content.setId(""+i);
            content.setTitle(i+"搜索工作其实很快乐");
            content.setPrice((float)(i*Math.random() + 1));
            content.setContent(i+"我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我"+
                    "们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开 始并扩展"+
                    "到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些"+
                    "问题和更多的问题。");
            client.prepareIndex("blog","article",i+"")
                    .setSource(objectMapper.writeValueAsString(content), XContentType.JSON).
                    get();
        }
        //关闭资源
        client.close();
    }

    @Test
    public void queryandsort() throws Exception{
        //创建客户端
          TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                  .addTransportAddress(new InetSocketTransportAddress
                          (InetAddress.getByName("127.0.0.1"),9300));

          //分页、排序查询
        SearchRequestBuilder searchRequestBuilder =
                client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery());
         int pageNo = 1;
         int pageSize = 5;
         if (pageNo<0){
             pageNo = 1;
         }
         //设置查询的起始条件
         searchRequestBuilder.setFrom((pageNo - 1)* pageSize);
        //设置每页大小
        searchRequestBuilder.setSize(pageSize);
        //排序(升序)
        searchRequestBuilder.addSort("price", SortOrder.ASC);
        //默认每页显示10条
        SearchResponse searchResponse = searchRequestBuilder.get();

        //获取查询结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("共查询" + hits.getTotalHits() + "条数据");
        for (SearchHit hit : hits) {
            System.out.println("查询结果" + hit.getSourceAsString());
            System.out.println("ID:" + hit.getSourceAsMap().get("id"));
            System.out.println("TITLE:" + hit.getSourceAsMap().get("title"));
            System.out.println("CONTENT:" + hit.getSourceAsMap().get("content"));
            System.out.println("Price:" + hit.getSourceAsMap().get("price"));
            System.out.println("===============================================");
        }

        //关闭资源
        client.close();
    }
}
