package com.luoningqi.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.luoningqi.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.luoningqi.gmall.realtime.common.constant.Constant;
import com.luoningqi.gmall.realtime.common.util.HBaseUtil;
import com.luoningqi.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{
    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection hBaseAsyncConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConnection = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConnection);
        HBaseUtil.closeAsyncHbaseConnection(hBaseAsyncConnection);

    }
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        String tableName = getTableName();
        String rowKey = getId(input);
        //java的异步编程方式
        String redisKey = RedisUtil.getRedisKey(tableName, rowKey);
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @SneakyThrows
            @Override
            public String get() {
                //第一步异步访问获取的数据
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(redisKey);
                return dimSkuInfoFuture.get();
            }
        }).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String dimInfo) {
                JSONObject dimJsonObj = null;
                //旁路缓存判断
                if (dimInfo==null||dimInfo.length()==0){
                    //需要访问Hbase
                    try {
                        dimJsonObj = HBaseUtil.getAsyncCells(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, tableName, rowKey);
                        //将读取的数据保存到redis
                        redisAsyncConnection.async().setex(redisKey, 24*60*60, dimJsonObj.toJSONString());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    //redis中存在缓存数据
                    dimJsonObj = JSONObject.parseObject(dimInfo);
                }
                return dimJsonObj;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject dim) {
                //合并维度信息
                if (dim==null){
                    //无法关联到维度信息
                    System.out.println("无法关联到当前的维度信息"+tableName+":"+ rowKey);
                } else {
                    join(input,dim);
                }
                //返回结果
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }
}
