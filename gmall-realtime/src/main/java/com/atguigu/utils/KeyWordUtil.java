package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeyWordUtil {

    public static List<String> splitKeyWord(String keyword) throws IOException {

        //创建集合用于存放最终结果数据
        ArrayList<String> list = new ArrayList<>();

        StringReader stringReader = new StringReader(keyword);

        //创建分词对象
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, false);

        Lexeme next = ikSegmenter.next();

        while (next != null) {
            String word = next.getLexemeText();
            list.add(word);
            next = ikSegmenter.next();
        }

        //返回集合
        return list;

    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyWord("Flink实时数仓"));
    }

}
