package com.luoningqi.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.luoningqi.gmall.realtime.common.bean.TableProcessDim;
import com.luoningqi.gmall.realtime.common.util.HBaseUtil;
import com.luoningqi.gmall.realtime.common.constant.Constant;
import com.luoningqi.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class DimHbaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    Connection connection;
    Jedis jedis;


    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeConnection(connection);
        RedisUtil.closeJedis(jedis);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObj = value.f0;
        TableProcessDim dim = value.f1;
        String type = jsonObj.getString("type");
        JSONObject data = jsonObj.getJSONObject("data");
        if ("delete".equals(type)) {
            //删除对应的维度表数据
            delete(data, dim);
        } else {
            //覆盖写入维度表数据
            put(data, dim);
        }
        //判断redis中的缓存是否发生变化
        if ("delete".equals(type)||"update".equals(type)){
            jedis.del(RedisUtil.getRedisKey(dim.getSinkTable(),data.getString(dim.getSinkRowKey())));
        }

    }

    private void delete(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        try {
            HBaseUtil.deleteCells(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKeyValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void put(JSONObject data, TableProcessDim dim) {
        System.out.println(10086);
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        String sinkFamily = dim.getSinkFamily();
        try {
            HBaseUtil.putCells(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKeyValue, sinkFamily, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}