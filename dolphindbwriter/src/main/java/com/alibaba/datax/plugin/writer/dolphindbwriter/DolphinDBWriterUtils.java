package com.alibaba.datax.plugin.writer.dolphindbwriter;

import com.alibaba.datax.common.util.Configuration;
import com.xxdb.DBConnection;
import org.apache.commons.lang3.StringUtils;

public class DolphinDBWriterUtils {

    protected static DBConnection createDBConnectionAndConnect(Configuration writerConfig) throws Exception{
        DBConnection dbConnection = new DBConnection();
        String userId = getUserIdFromWriterConfig(writerConfig);
        String pwd = getPwdFromWriterConfig(writerConfig);
        dbConnection.connect(
                writerConfig.getString(Key.HOST),
                writerConfig.getInt(Key.PORT),
                userId,
                pwd
        );

        return dbConnection;
    }

    protected static String getUserIdFromWriterConfig(Configuration writerConfig) {
        String configUserId = writerConfig.getString(Key.USER_ID);
        String configUsername = writerConfig.getString(Key.USERNAME);
        if (StringUtils.isNotEmpty(configUsername) || StringUtils.isNotEmpty(configUserId))
            return StringUtils.isNotEmpty(configUsername) ? configUsername : configUserId;
        else
            throw new RuntimeException("The param 'userId' or 'username' cannot be null in writerConfig.");
    }

    protected static String getPwdFromWriterConfig(Configuration writerConfig) {
        String configPwd = writerConfig.getString(Key.PWD);
        String configPassword = writerConfig.getString(Key.PASSWORD);
        if (StringUtils.isNotEmpty(configPassword) || StringUtils.isNotEmpty(configPwd))
            return StringUtils.isNotEmpty(configPassword) ? configPassword : configPwd;
        else
            throw new RuntimeException("The param 'pwd' or 'password' cannot be null in writerConfig.");
    }

}
