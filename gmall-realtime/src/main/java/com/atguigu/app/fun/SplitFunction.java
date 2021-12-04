package com.atguigu.app.fun;

import com.atguigu.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))

public class SplitFunction extends TableFunction {

    public void eval(String str) {

        try {
            List<String> list = KeyWordUtil.splitKeyWord(str);

            for (String word : list) {
                collect(Row.of(word));
            }
        } catch (IOException e) {

            System.out.println("发现异常数据：" + str);
        }
    }

}
