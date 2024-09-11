package com.luoningqi.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class HBaseUtil {
    /**
     * 获取hbase连接
     *
     * @return 一个HBase的同步连接
     */
    public static Connection getConnection() {
        //使用conf
//        Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
        //使用配置文件
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }

    /**
     * 关闭HBase连接
     *
     * @param connection 一个HBase的同步连接
     */
    public static void closeConnection(Connection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    /**
     * 创建表格
     *
     * @param connection 一个HBase的同步连接
     * @param namespace  命名空间名称
     * @param table      表名
     * @param families   列族名 可以是多个
     * @throws IOException 获取admin连接异常
     */
    public static void createTable(Connection connection, String namespace, String table, String... families) throws IOException {
        if (families == null || families.length == 0) {
            System.out.println("创建hbase表格至少有一个列族");
            return;
        }
        //1.获取admin
        Admin admin = connection.getAdmin();
        //2.创建表格描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));

        for (String family : families) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }
        //3.使用admin调用方法创建表格
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("当前表格已经存在 不需要重复创建" + namespace + ":" + table);
        }
        //4.关闭admin
        admin.close();

    }

    /**
     * 删除表格
     *
     * @param connection 一个HBase的同步连接
     * @param namespace  命名空间
     * @param table      表名
     * @throws IOException 获取admin连接异常
     */
    public static void dropTable(Connection connection, String namespace, String table) throws IOException {
        //1.获取admin
        Admin admin = connection.getAdmin();

        //2.调用方法删除表格
        try {
            admin.disableTable(TableName.valueOf(namespace, table));
            admin.deleteTable(TableName.valueOf(namespace, table));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //3.关闭admin
        admin.close();

    }

    /**
     * 写数据到hbase的方法
     *
     * @param connection 一个同步连接
     * @param namespace  命名空间
     * @param tableName  表名
     * @param rowKey     主键
     * @param family     列族名
     * @param data       列名和列值(JSONobj对象)
     */
    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data) throws IOException {
        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //2.创建写入对象
        Put put = new Put(Bytes.toBytes(rowKey));

        for (String column : data.keySet()) {
            String columnValue = data.getString(column);
            if (columnValue != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(columnValue));
            }
        }

        //3.调用方法写出数据
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //4.关闭table
        table.close();

    }

    public static JSONObject getCells(Connection connection, String namespace, String tablename, String rowkey) throws IOException {
        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tablename));
        //2.创建 get 对象
        Get get = new Get(Bytes.toBytes(rowkey));
        JSONObject jsonObject = new JSONObject();
        //3.调用 get 方法
        try{
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                jsonObject.put(new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8),new String(CellUtil.cloneValue(cell),StandardCharsets.UTF_8));
            }
        } catch (IOException exception){
            exception.printStackTrace();
        }

        //4.关闭table
        table.close();

        return jsonObject;

    }

    public static JSONObject getAsyncCells(AsyncConnection hBaseAsyncConnection, String namespace, String tablename, String rowkey) throws IOException {
        //1.获取table
        AsyncTable<AdvancedScanResultConsumer> table = hBaseAsyncConnection.getTable(TableName.valueOf(namespace, tablename));
        //2.创建 get 对象
        Get get = new Get(Bytes.toBytes(rowkey));
        JSONObject jsonObject = new JSONObject();
        //3.调用 get 方法
        try{
            Result result = table.get(get).get();
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                jsonObject.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (Exception exception){
            exception.printStackTrace();
        }

        return jsonObject;

    }

    /**
     * 删除一整行数据
     *
     * @param connection 一个同步连接
     * @param namespace  命名空间名称
     * @param tablename  表名
     * @param rowKey     主键行号
     * @throws IOException
     */
    public static void deleteCells(Connection connection, String namespace, String tablename, String rowKey) throws IOException {
        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tablename));
        //2.创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //3.调用方法删除数据
        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //4.关闭table
        table.close();
    }
}
