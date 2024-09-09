package com.luoningqi.gmall.realtime.dws.function;

import com.luoningqi.gmall.realtime.common.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import java.util.Set;

@FunctionHint(output = @DataTypeHint("row<keyword string>"))
public class KwSplit extends TableFunction<Row> {
    public void eval(String keywords) {
        if (keywords == null) {
            return;
        }
        // "华为手机白色手机"
        Set<String> stringList = IkUtil.split(keywords);
        for (String s : stringList) {
            collect(Row.of(s));
        }
    }
}
