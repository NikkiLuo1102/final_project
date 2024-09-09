package com.luoningqi.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.luoningqi.gmall.realtime.common.bean.TradeSkuOrderBean;

public interface DimJoinFunction<T> {
    public String getId(T input);
    public String getTableName();
    public void join(T input, JSONObject dim);
}
