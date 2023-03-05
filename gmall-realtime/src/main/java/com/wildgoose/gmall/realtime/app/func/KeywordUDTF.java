package com.wildgoose.gmall.realtime.app.func;

import com.wildgoose.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author 张翔
 * @date 2023/3/4 16:38
 * @description
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String s : KeywordUtil.analyze(text)) {
            collect(Row.of(s));
        }
    }
}
