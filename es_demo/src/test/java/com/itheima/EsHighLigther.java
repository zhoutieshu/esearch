package com.itheima;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;

public class EsHighLigther {
    @Test
    public void searchByHighLigther() throws Exception {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(
                        InetAddress.getByName("127.0.0.1"), 9300));
        //搜索数据
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("blog3")
                .setTypes("article").setQuery(QueryBuilders.termQuery("title", "搜索"));

        //设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.field("title");
        highlightBuilder.postTags("</font>");
        searchRequestBuilder.highlighter(highlightBuilder);

        //获取查询结果
        SearchResponse searchResponse = searchRequestBuilder.get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("共搜索到" + hits.totalHits + "条结果");
        for (SearchHit hit : hits) {
            System.out.println("Sting方式打印文档搜索结果");
            System.out.println(hit.getSourceAsString());
            System.out.println("Map方式打印高亮结果");
            System.out.println(hit.getHighlightFields());
            System.out.println("遍历高亮结果，打印高亮片段");
            Text[] titles = hit.getHighlightFields().get("title").getFragments();
            StringBuilder builder = new StringBuilder();
            for (Text text : titles) {
                builder.append(text.string());
            }
            System.out.println(builder.toString());
            System.out.println("==================================");
        }
        //关闭资源
        client.close();
    }
}
