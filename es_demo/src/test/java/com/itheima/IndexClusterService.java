package com.itheima;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.itheima.domain.Content;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
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

public class IndexClusterService {


    // 创建客户端对象
    private TransportClient transportClient;


    @Before
    public void initClient() throws  Exception{
        // 注意：******************************如果你修改了集群名字，就必须使用下面的方式去覆盖默认的配置**********************************
        Settings settings = Settings.builder().put("cluster.name","cluster-es").build();
        // 2、建立es的客户端对象
        transportClient = new PreBuiltTransportClient(settings);
        // 3、设置es服务器的iP和端口
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
    }

    @Test
    public void createIndexRepo(){
        // 创建索引库
        CreateIndexResponse response =  transportClient.admin().indices().prepareCreate("blog3").get();
        System.out.println(response.isAcknowledged());
        // 关闭transportClient
        transportClient.close();
    }



    // 注意1：规则一定是在创建索引库以后立即去执行和创建事情，不能等索引库存在数据在修改。
    // 注意2：如果你使用了analyzer ，前提是在es服务中必须配置了ik插件，否则报异常
    // 注意3：创建规则的时候，不会自动创建索引库和类型。
    @Test
    public void createMapping() throws  Exception{
        CreateIndexResponse response =  transportClient.admin().indices().prepareCreate("blog").get();
        if(response.isAcknowledged()) {
            // 1：组装mapping规则
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("content")
                    .startObject("properties")
                    .startObject("id").field("type", "long").field("store", true).endObject()
                    // ******************************如果你使用了analyzer ，前提是在es服务中必须配置了ik插件，否则报异常**********
                    .startObject("title").field("type", "text").field("store", true).field("analyzer", "ik_smart").endObject()
                    .startObject("content").field("type", "text").field("store", true).field("analyzer", "ik_smart").endObject()
                    .startObject("price").field("type", "double").field("store", true).endObject()
                    .endObject()
                    .endObject()
                    .endObject();


            //http://localhost:9200/blog/content/_mapping?text=json------PUT
            // 2:创建索引库blog的types为content的Mapping规则
            PutMappingResponse putMappingResponse = transportClient.admin().indices()
                    .preparePutMapping("blog").setType("content")
                    //  添加和规则的关系
                    .setSource(xContentBuilder)
                    .get();
            // 生成
            System.out.println(putMappingResponse.isAcknowledged());
            // 3: 关闭transportClient
            transportClient.close();
        }
    }



    // 3: 创建/修改文档 {id:1,title:"23423",content:"23423",price:23}
    @Test
    public void createIndex() throws  Exception{

        for (int i = 1; i <= 50; i++) {
            // 1: 组装数据
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                    .field("id",i)
                    .field("price",80+i)
                    .field("title","elasticsearch是一个基于lucene的搜索服务--"+i)
                    .field("content","ElasticSearch是一个基于Lucene的搜索服务器。-" +
                            "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。" +
                            "Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，" +
                            "是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，" +
                            "可靠，快速，安装使用方便。"+i)
                    .endObject();


            // 创建文档document
            //IndexResponse indexResponse = transportClient.prepareIndex("blog", "content")
            //       .setSource(contentBuilder).get();

            IndexResponse indexResponse = transportClient.prepareIndex("blog", "content",i+"")
                    .setSource(contentBuilder).get();

            System.out.println(indexResponse.getIndex());
        }
        // 关闭transportClient
        transportClient.close();

    }



    @Test
    public void createIndexByPojo() throws  Exception{
        // 1: 组装数据
        for (long i = 51; i <= 100; i++) {
            Content content = new Content();
            content.setId("2");
            content.setTitle("solr-是一个基于lucene的搜索服务"+i);
            content.setContent("solr是一个基于Lucene的搜索服务器。-update\" +\n" +
                    "                        \"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。\" +\n" +
                    "                        \"Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，\" +\n" +
                    "                        \"是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，\" +\n" +
                    "                        \"可靠，快速，安装使用方便。"+i);
            content.setPrice(90f+i);


            // 把对象转换成json字符串
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(content);

            // 然后创建文档和库和表中，如果没有指定id，那么就是使用UUID作为主键值，建议是指定。
            IndexResponse indexResponse = transportClient.prepareIndex("blog", "content",content.getId()+"")
                    .setSource(json,XContentType.JSON).get();

            System.out.println(indexResponse.getIndex());

        }

        // 关闭transportClient
        transportClient.close();


    }





    // 删除索引
    @Test
    public void deleteIndex(){
        // 1：删除
        DeleteResponse deleteResponse = transportClient.prepareDelete("blog", "content", "AWvQ3infXmrbbNnZuZG2").get();
        System.out.println(deleteResponse.status());
        // 2：关闭transportClient
        transportClient.close();

    }



}
