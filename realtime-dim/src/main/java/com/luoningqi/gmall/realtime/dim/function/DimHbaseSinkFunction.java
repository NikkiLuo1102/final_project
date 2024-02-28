package com.luoningqi.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.luoningqi.gmall.realtime.common.bean.TableProcessDim;
import com.luoningqi.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

import static com.luoningqi.gmall.realtime.common.constant.Constant.HBASE_NAMESPACE;

public class DimHbaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeConnection(connection);
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

    }

    private void delete(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        try {
            HBaseUtil.deleteCells(connection, HBASE_NAMESPACE, sinkTable, sinkRowKeyValue);
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
            HBaseUtil.putCells(connection, HBASE_NAMESPACE, sinkTable, sinkRowKeyValue, sinkFamily, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}