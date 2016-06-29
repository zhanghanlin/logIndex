package com.cheyipai.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.cheyipai.AbstractTest;
import com.cheyipai.elasticsearch.cron.Data2ElasticTimer;
import com.cheyipai.elasticsearch.service.HbaseService;
import com.cheyipai.elasticsearch.utils.DateUtils;
import org.junit.Test;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

import static com.cheyipai.elasticsearch.common.Config.hbase_biglog_prefix;

public class ElasticTest extends AbstractTest {

    @Resource
    Data2ElasticTimer data2ElasticTimer;

    @Resource
    HbaseService hbaseService;


    @Test
    public void testFind() {
        List<Map<String, Object>> list = hbaseService.find(
                hbase_biglog_prefix + DateUtils.getMonthDate(), "1467108075983");
        System.out.println(list.size());
        for (Map map : list) {
            System.out.println(JSONObject.toJSONString(map));
        }
    }
}
