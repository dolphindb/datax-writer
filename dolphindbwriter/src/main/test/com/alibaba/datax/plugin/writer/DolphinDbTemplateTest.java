package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.plugin.writer.dolphindbwriter.DolphinDbTemplate;

public class DolphinDbTemplateTest {
    public static void main(String[] args) {
//        DolphinDbTemplate.readScript("dimensionTableUpdateScript.dos");
        DolphinDbTemplate.readScript("./dimensionTableUpdateScript.dos");
    }
}
