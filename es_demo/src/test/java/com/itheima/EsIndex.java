package com.itheima;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class EsIndex {
    private TransportClient transportClient;

    @Before
    public void createClient() throws UnknownHostException {

        //如果修改了集群的名字,就必须使用的方式去覆盖默认的配置
        Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
        //创建一个客户端对象
         transportClient = new PreBuiltTransportClient(settings);
         //设置集群信息
          transportClient.addTransportAddress(
                  new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        transportClient.addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        transportClient.addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
    }

    /**
     * 创建索引
     */
    @Test
    public void createIndex(){
        //使用客户端对象完成索引库的创建
        CreateIndexResponse response = transportClient.admin().indices().prepareCreate("blog3").get();
        System.out.println(response.isShardsAcked());
        System.out.println(transportClient);
      transportClient.close();
    }


    /**
     * 配置映射
     * @throws Exception
     */
    @Test
    public void  createMapping() throws Exception{
        XContentBuilder contentBuilder =  XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id").field("type","long")
                .field("store",true).endObject()
                .startObject("title").field("type","text")
                .field("store",true).field("analyzer","ik_smart").endObject()
                .startObject("content").field("type","text")
                .field("store",true).field("analyzer","ik_smart").endObject()
                .endObject()
                .endObject()
                .endObject();


        transportClient.admin().indices()
                // 设置要做映射的索引
                .preparePutMapping("blog2")
                // 设置做映射的type
                .setType("article")
                // 设置mapping信息，可以是XContentBuilder 也可以是json字符串
                .setSource(contentBuilder).get();


        transportClient.close();

    }


}
