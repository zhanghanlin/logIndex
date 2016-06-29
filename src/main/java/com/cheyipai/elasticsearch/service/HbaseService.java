package com.cheyipai.elasticsearch.service;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.cheyipai.elasticsearch.common.Config.hbase_date_offset;

@Service
public class HbaseService {

    @Resource
    HbaseTemplate hbaseTemplate;

    /**
     * 通过表名，开始行键和结束行键获取数据
     *
     * @return
     */
    public List<Map<String, Object>> find(String tableName, String... args) {
        Scan scan = new Scan();
        if (args != null && args.length > 0) {
            if (StringUtils.isNotBlank(args[0])) {
                Long start = Long.valueOf(args[0]);
                Long end = (new Date()).getTime() + hbase_date_offset;
                if (args.length > 1 && StringUtils.isNotBlank(args[1])) {
                    end = Long.valueOf(args[1]);
                }
                if (start < end) {
                    try {
                        scan.setTimeRange(start, end);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return hbaseTemplate.find(tableName, scan, new RowMapper<Map<String, Object>>() {
            public Map<String, Object> mapRow(Result result, int rowNum) throws Exception {
                return execMapRow(result);
            }
        });
    }

    /**
     * Result2Map
     *
     * @param result
     * @return
     */
    Map<String, Object> execMapRow(Result result) {
        List<Cell> ceList = result.listCells();
        Map<String, Object> map = Maps.newConcurrentMap();
        String row = "";
        if (ceList != null && ceList.size() > 0) {
            long timestamp = 0L;
            for (Cell cell : ceList) {
                row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                map.put(qualifier, value);
                timestamp = cell.getTimestamp();
            }
            map.put("rowKey", row);
            map.put("timestamp", timestamp);
        }
        return map;
    }
}
