package com.cheyipai.elasticsearch.service;

import com.cheyipai.elasticsearch.utils.BDBHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.cheyipai.elasticsearch.common.Config.bdb_table_key;
import static com.cheyipai.elasticsearch.common.Config.bdb_table_name;

@Service
public class ElasticService {

    private static final Log LOG = LogFactory.getLog(ElasticService.class);

    @Resource
    private ElasticsearchTemplate elasticsearchTemplate;

    /**
     * 批量插入
     *
     * @param list
     * @throws IOException
     */
    public void bulk(List<Map<String, Object>> list, String index, String type) throws IOException {
        Client client = elasticsearchTemplate.getClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        Long timestamp = 0L;
        for (Map<String, Object> map : list) {
            Long timestamp_temp = Long.valueOf(map.get("timestamp").toString());
            Long logTime = Long.valueOf(map.get("logTime").toString());
            map.put("timestamp", new Date(timestamp_temp));
            map.put("logTime", new Date(logTime));
            String rowKey = map.get("rowKey").toString();
            if (timestamp < timestamp_temp) {
                timestamp = timestamp_temp;
            }
            bulkRequest.add(client.prepareIndex(index, type, rowKey).setSource(map));
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            LOG.error("bulk index : " + index + ",type :" + type + " error! "
                    + bulkResponse.buildFailureMessage());
        }
        BDBHandler bdbHandler = BDBHandler.getInstance();
        bdbHandler.put(bdb_table_key, timestamp.toString(), bdb_table_name);
    }
}
