package com.cheyipai.elasticsearch.common;

import com.cheyipai.elasticsearch.common.config.Global;

public class Config {

    /**
     * bigLog Hbase表名前缀
     */
    public static String hbase_biglog_prefix;

    /**
     * HBase时间不同步设置偏移量
     */
    public static Long hbase_date_offset = 0L;

    /**
     * BDB Key
     */
    public static String bdb_table_key;

    /**
     * BDB Name
     */
    public static String bdb_table_name;

    /**
     * 定时任务执行间隔
     */
    public static Long timer_timeOut;

    public static String elastic_index_prefix;
    public static String elastic_type;

    static {
        elastic_index_prefix = Global.getConfig("elastic.index.prefix");
        elastic_type = Global.getConfig("elastic.type");
        hbase_biglog_prefix = Global.getConfig("hbase.biglog.prefix");
        bdb_table_key = Global.getConfig("bdb.table.key");
        bdb_table_name = Global.getConfig("bdb.table.name");
        timer_timeOut = Long.valueOf(Global.getConfig("timer.timeout"));
        hbase_date_offset = Long.valueOf(Global.getConfig("hbase.date.offset"));
    }
}
