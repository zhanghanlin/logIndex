package com.cheyipai.elasticsearch.utils;

import com.cheyipai.elasticsearch.common.config.Global;
import com.sleepycat.je.*;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class BDBHandler {
    private Environment env;
    private Database db;
    private long cacheSize = 8 * 1024 * 1024;
    private String path = "/usr/local/bdb/";
    private static BDBHandler instance = new BDBHandler();

    /**
     * Init
     */
    public static BDBHandler getInstance() {
        return instance;
    }

    /**
     * 初始化BDB属性值
     */
    private BDBHandler() {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);   //当前环境支持事务处理，默认为false,不支持事务处理
        envConfig.setTxnWriteNoSyncVoid(true);    //提交事务的时候是否把缓冲的log写到磁盘上。true表示不同步，也就是说不写磁盘
        envConfig.setAllowCreate(true);     //当数据库环境不存在时候重新创建一个数据库环境，默认为false
        envConfig.setCacheSize(cacheSize);  //当前环境能够使用的最大RAM.单位BYTE
        try {
            path = Global.getConfig("bdb.table.path");
            File f = new File(path);
            if (!f.exists()) f.mkdirs();
            env = new Environment(new File(path), envConfig);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get Key
     *
     * @param key
     * @return
     */
    public List<String> get(String key) {
        ArrayList<String> nuclearStorageValue = new ArrayList<String>();
        DatabaseEntry queryKey = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        queryKey.setData(key.getBytes());
        Cursor cursor = null;
        try {
            cursor = db.openCursor(null, null);
            //通过key的方式检索，使用后游标指针将移动到跟当前key匹配的第一项
            //READ_UNCOMMITTED:读取修改但尚未提交的数据
            for (OperationStatus status = cursor.getSearchKey(queryKey, value,
                    LockMode.READ_UNCOMMITTED); status == OperationStatus.SUCCESS; status = cursor
                    .getNextDup(queryKey, value, LockMode.RMW)) {
                nuclearStorageValue.add(new String(value.getData()));
            }
        } catch (DatabaseException e) {
            e.printStackTrace();
        } finally {
            attemptClose(cursor);
        }
        return nuclearStorageValue;
    }

    /**
     * Open BDB
     *
     * @param dbName 数据库名
     */
    public void open(String dbName) {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);      //打开一个数据库，如果数据库不存在则创建一个
        dbConfig.setTransactional(true);    //当前数据库支持事务处理，默认为false,不支持事务处理
        try {
            //打开一个数据库，数据库名为dbName,数据库的配置为dbConfig
            db = env.openDatabase(null, dbName, dbConfig);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    /**
     * 遍历数据库中的所有记录，返回list
     */
    public ArrayList<String> getEveryItem() {
        Cursor cursor = null;
        ArrayList<String> resultList = new ArrayList<String>();
        Transaction txn = null;
        try {
            txn = env.beginTransaction(null, null);    //获取事务
            cursor = db.openCursor(txn, null);  //事务游标-->创建游标时提供事务句柄
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();
            if (cursor.getFirst(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                String theKey = new String(foundKey.getData(), "UTF-8");
                resultList.add(theKey);
                while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                    theKey = new String(foundKey.getData(), "UTF-8");
                    resultList.add(theKey);
                }
            }
            cursor.close();
            txn.commit();
            return resultList;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            resultList = null;
        } catch (Exception e) {
            txn.abort();
            if (cursor != null) {
                cursor.close();
            }
            resultList = null;
        } finally {
            attemptClose(cursor);
        }
        return resultList;
    }

    /**
     * Set Key:value
     *
     * @param key
     * @param value
     */
    public void put(String key, String value) {
        byte[] theKey = key.getBytes();
        byte[] theValue = value.getBytes();
        OperationStatus status = null;
        Transaction transaction = null;
        Cursor cursor = null;
        boolean succeeded = false;
        try {
            transaction = env.beginTransaction(null, null);
            cursor = db.openCursor(transaction, null);      //事务游标-->创建游标时提供事务句柄
            status = cursor.put(new DatabaseEntry(theKey), new DatabaseEntry(theValue));
            if (status != OperationStatus.SUCCESS) {
                //Success
            }
            succeeded = true;
        } catch (DatabaseException e) {
            e.printStackTrace();
        } finally {
            attemptClose(cursor);
            if (succeeded)
                attemptCommit(transaction);
            else
                attemptAbort(transaction);
        }
    }

    /**
     * 删除数据库中的一条记录
     *
     * @param dbName 数据库名称
     * @param key    需要删除的Key
     * @return
     */
    public boolean deleteFromDatabase(String dbName, String key) {
        Transaction transaction = null;
        boolean succeeded = false;
        try {
            transaction = env.beginTransaction(null, null);
            DatabaseEntry theKey;
            theKey = new DatabaseEntry(key.getBytes());
            OperationStatus res = db.delete(transaction, theKey);
            transaction.commit();
            if (res == OperationStatus.SUCCESS) {
                succeeded = true;
                return succeeded;
            } else if (res == OperationStatus.KEYEMPTY) {
                System.out.println("没有从数据库" + dbName + "中找到:" + key + "。无法删除");
            } else {
                System.out.println("删除操作失败，由于" + res.toString());
            }
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            succeeded = false;
        } finally {
            if (!succeeded) {
                if (transaction != null) {
                    transaction.abort();
                }
            }
        }
        return succeeded;
    }

    /**
     * 清理日志
     */
    public void cleanLog() {
        CheckpointConfig ckptConfig = new CheckpointConfig();
        ckptConfig.setMinutesVoid(3);
        env.checkpoint(ckptConfig);
        env.cleanLog();
    }

    /**
     * 关闭游标
     *
     * @param cursor
     */
    private static void attemptClose(Cursor cursor) {
        try {
            if (cursor != null)
                cursor.close();
        } catch (DatabaseException e) {
        }
    }

    /**
     * 提交事物
     *
     * @param transaction
     */
    private static void attemptCommit(Transaction transaction) {
        try {
            transaction.commit();
        } catch (DatabaseException e) {
            attemptAbort(transaction);
            e.printStackTrace();
            System.out.println("提交事物失败!");
        }
    }

    /**
     * 退出事物 - 任何提交或取消的事务都不能再被使用
     *
     * @param transaction
     */
    private static void attemptAbort(Transaction transaction) {
        try {
            transaction.abort();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void put(String key, String value, String dbName) {
        open(dbName);
        put(key, value);
    }

    public String get(String key, String dbName) {
        open(dbName);
        List<String> vals = get(key);
        if (vals != null && !vals.isEmpty()) {
            return vals.get(0);
        }
        return null;
    }
}
