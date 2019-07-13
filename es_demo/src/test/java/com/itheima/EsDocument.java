package com.itheima;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itheima.domain.Article;
import com.itheima.domain.Content;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;



public class EsDocument {

    private TransportClient client;

    @Before
    public void init() throws Exception {
        client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress
                        (InetAddress.getByName("127.0.0.1"), 9300));
    }
    /**
     * 创建文档(方式一)
     */
    @Test
    public void createIndex1() throws Exception {

        //创建文档
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("id", 1)
                .field("title", "elasticsearch是一个基于lucene的搜索服务")
                .field("content", "ElasticSearch是一个基于Lucene的搜索服务器。" +
                        "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。" +
                        "Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，" +
                        "是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，" +
                        "可靠，快速，安装使用方便。")
                .endObject();
        //创建索引
        client.prepareIndex("blog3","article","1")
               .setSource(contentBuilder).get();

        client.close();
    }
    /**
     * 方式二
     */
    @Test
    public void addIndex2() throws JsonProcessingException {
        //创建一个文档对象
        Article article = new Article();
        article.setId(3L);
        article.setTitle("js");
        article.setContent("js content");

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(article);

        //把文档对象添加到索引库
        IndexResponse response = client.prepareIndex()
                .setIndex("blog2")
                .setType("article")
                .setId(article.getId()+"")
                .setSource(json, XContentType.JSON).get();
        System.out.println(response.status());
        client.close();


    }

    /**
     *修改文档
     */
    @Test
    public void updateIndex() throws  Exception{
//修改
        Content content = new Content();
        content.setId("3");
        content.setTitle("搜索工作其实很快乐xxx");
        content.setContent("33333我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我们希"+
                "望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开 始并扩展到数"+
                "百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些问题"+
                "和更多的问题。");

        ObjectMapper mapper = new ObjectMapper();
        UpdateResponse response = client.prepareUpdate("blog2","article",content.getId()).
                setDoc(mapper.writeValueAsString(content),XContentType.JSON).get();
        System.out.println(response.status());
        //关闭资源
        client.close();
    }

    /**
     * 删除文档
     */
    @Test
    public void deleteIndex() throws Exception {
    /*    //删除文档
        client.prepareDelete("blog2", "article", "3").get();
        //关闭资源
        client.close();*/

        //删除文档
        client.delete(new DeleteRequest("blog2", "article", "3")).get();
//关闭资源
        client.close();
    }
}
