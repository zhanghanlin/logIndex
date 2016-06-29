package com.cheyipai.elasticsearch.cron;

import com.cheyipai.elasticsearch.service.ElasticService;
import com.cheyipai.elasticsearch.service.HbaseService;
import com.cheyipai.elasticsearch.utils.BDBHandler;
import com.cheyipai.elasticsearch.utils.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.cheyipai.elasticsearch.common.Config.*;
import static com.cheyipai.elasticsearch.utils.DateUtils.getMonthDate;

@Service
public class Data2ElasticTimer {

    private static final Log LOG = LogFactory.getLog(Data2ElasticTimer.class);

    Lock lock = new ReentrantLock();

    @Resource
    HbaseService hbaseService;

    @Resource
    ElasticService elasticService;

    /**
     * 查询数据并写入Elastic
     *
     * @param tableName HBase表名
     * @throws IOException
     */
    void putData(String tableName) throws IOException {
        BDBHandler bdbHandler = BDBHandler.getInstance();
        //offset记录最后一次查询的数据时间戳
        String start = bdbHandler.get(bdb_table_key, bdb_table_name);
        List<Map<String, Object>> list = hbaseService.find(tableName, start);
        LOG.info("HBaseService find start : " + start + ", list size : " + list.size());
        if (list != null && !list.isEmpty()) {
            String indexName = elastic_index_prefix + "." + DateUtils.getMonthDate();
            elasticService.bulk(list, indexName, elastic_type);
        }
    }

    /**
     * HBase数据写入Elastic
     */
    void data2Elastic() {
        lock.lock();
        try {
            putData(hbase_biglog_prefix + getMonthDate());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    @PostConstruct
    public void timer() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                data2Elastic();
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, 1000, timer_timeOut);
    }
}
