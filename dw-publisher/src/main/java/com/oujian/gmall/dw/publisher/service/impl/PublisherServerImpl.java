package com.oujian.gmall.dw.publisher.service.impl;

import com.oujian.gmall.dw.common.constants.GmallConstants;
import com.oujian.gmall.dw.publisher.service.PublisherServer;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.Aggregation;
import io.searchbox.core.search.aggregation.SumAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServerImpl implements PublisherServer {
    @Autowired
    private JestClient jestClient;

    @Override
    public long getDauTotal(String date) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        QueryBuilder queryBuilder = new MatchQueryBuilder("logDate",date);
        BoolQueryBuilder filter = boolQueryBuilder.filter(queryBuilder);

        SearchSourceBuilder query = searchSourceBuilder.query(filter);
        Search search = new Search.Builder(query.toString())
                .addIndex(GmallConstants.ES_INDEX_DAU)
                .addType("_doc")
                .build();
        SearchResult execute = jestClient.execute(search);
        Long total = execute.getTotal();
        return total;
    }



    @Override
    public Map<String,Long> getItem(String date){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        QueryBuilder queryBuilder = new MatchQueryBuilder("logDate",date);
        BoolQueryBuilder filter = boolQueryBuilder.filter(queryBuilder);
        SearchSourceBuilder query = searchSourceBuilder.query(filter);
        TermsAggregationBuilder logHour = AggregationBuilders.terms("groupby_logHour").field("logHour");
        query.aggregation(logHour);
        Search search = new Search.Builder(query.toString())
                .addIndex(GmallConstants.ES_INDEX_DAU)
                .addType("_doc")
                .build();
        HashMap<String, Long> result = new HashMap<>();
        try {
            SearchResult execute = jestClient.execute(search);
            TermsAggregation groupbyLogHour = execute.getAggregations().getTermsAggregation("groupby_logHour");
            List<TermsAggregation.Entry> buckets = groupbyLogHour.getBuckets();
            for(TermsAggregation.Entry ter:buckets){
                result.put(ter.getKey(),ter.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public HashMap<String, Double> getNewOrderInfo(String date) {
        HashMap<String, Double> map = new HashMap<>();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder createHour = new MatchQueryBuilder("createDate", date);
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders
                .terms("group_CreateHour").field("createHour");
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("sum_totalAmount").field("totalAmount");
        TermsAggregationBuilder aggregationBuilder1 = aggregationBuilder.subAggregation(sumAggregationBuilder);
        BoolQueryBuilder filter = boolQueryBuilder.filter(createHour);
        SearchSourceBuilder aggregation = searchSourceBuilder.query(filter).aggregation(aggregationBuilder1);
        Search search = new Search.Builder(aggregation.toString())
                .addIndex(GmallConstants.ES_INDEX_NEW_ORDER)
                .addType("_doc")
                .build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            TermsAggregation termsAggregation = searchResult.getAggregations().getTermsAggregation("group_CreateHour");
            List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();
            for(TermsAggregation.Entry entry:buckets){
                Double sumTotalAmount = entry.getSumAggregation("sum_totalAmount").getSum();
                map.put(entry.getKey(),sumTotalAmount);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    @Override
    public long getNewOrderTotal(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder createDate = new MatchQueryBuilder("createDate", date);
        SearchSourceBuilder query = searchSourceBuilder.query(boolQueryBuilder.filter(createDate));
        Search doc = new Search.Builder(query.toString())
                .addType("_doc")
                .addIndex(GmallConstants.ES_INDEX_NEW_ORDER)
                .build();
        try {
            SearchResult execute = jestClient.execute(doc);
            return execute.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public Map<String, Long> getNewHourOrder(String date) {
        HashMap<String, Long> map = new HashMap<>();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("createDate", date);
        BoolQueryBuilder filter = boolQueryBuilder.filter(matchQueryBuilder);
        TermsAggregationBuilder group = AggregationBuilders.terms("groupby_creatDate").field("createHour");
        SearchSourceBuilder aggregation = searchSourceBuilder.query(filter).aggregation(group);
        Search doc = new Search.Builder(aggregation.toString()).addIndex(GmallConstants.ES_INDEX_NEW_ORDER)
                .addType("_doc").build();
        try {
            SearchResult execute = jestClient.execute(doc);
            TermsAggregation groupbyCreatDate = execute.getAggregations().getTermsAggregation("groupby_creatDate");
            List<TermsAggregation.Entry> buckets = groupbyCreatDate.getBuckets();
            for(TermsAggregation.Entry entry:buckets){
                map.put(entry.getKey(),entry.getCount());

            }
            return map;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    @Override
    public Double getTotalAmount(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("createDate", date);
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("sum_totalAmount").field("totalAmount");
        BoolQueryBuilder filter = boolQueryBuilder.filter(matchQueryBuilder);
        SearchSourceBuilder aggregation = searchSourceBuilder.query(filter).aggregation(sumAggregationBuilder);
        Search doc = new Search.Builder(aggregation.toString()).addIndex(GmallConstants.ES_INDEX_NEW_ORDER)
                .addType("_doc").build();
        System.out.println(aggregation.toString());
        try {
            SearchResult execute = jestClient.execute(doc);
            Double sum_totalAmount = execute.getAggregations().getSumAggregation("sum_totalAmount").getSum();
            return sum_totalAmount;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return 0.0;
    }
}
