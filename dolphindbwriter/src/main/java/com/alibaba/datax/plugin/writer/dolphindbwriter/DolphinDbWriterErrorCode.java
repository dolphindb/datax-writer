package com.alibaba.datax.plugin.writer.dolphindbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * dolphindb业务错误码
 *
 * Created by apple on 2020/2/7.
 */
public enum DolphinDbWriterErrorCode implements ErrorCode{

    REQUIRED_VALUE("DolphinDbWriter-00", "您缺失了必须填写的参数值.")
    ;

    private final String code;
    private final String description;

    DolphinDbWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
