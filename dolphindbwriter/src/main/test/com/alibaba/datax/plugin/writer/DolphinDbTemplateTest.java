package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.plugin.writer.dolphindbwriter.DolphinDbTemplate;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;

public class DolphinDbTemplateTest {
    public static void main(String[] args) {
        String s = "你好";
        byte[] b = s.getBytes();
        byte[] b1 = Arrays.copyOf(b,b.length-2);
        String res = new String(b1);
        System.out.println(res);
    }
}
