/**
 * Impelement of DolphinDbWriter job and task.
 *
 * @author DolphinDB
 */
package com.alibaba.datax.plugin.writer.dolphindbwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.io.Long2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class DolphinDbWriter extends Writer {

    private static final String DOLPHINDB_DATAX_WRITER_VERSION = "1.30.22.3";

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(writerConfig);
            }
            return configurations;
        }

        @Override
        public void init() {
            this.writerConfig = this.getPluginJobConf();
            this.validateParameter();
            LOG.info("Dolphindb Writer config params:{}", this.writerConfig.toJSON());
        }

        @Override
        public void destroy() {

        }

        private void validateParameter() {
            this.writerConfig.getNecessaryValue(Key.HOST, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            this.writerConfig.getNecessaryValue(Key.PORT, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            if (StringUtils.isEmpty(this.writerConfig.getString(Key.PASSWORD)) && StringUtils.isEmpty(this.writerConfig.getString(Key.PWD))) {
                throw DataXException.asDataXException(DolphinDbWriterErrorCode.REQUIRED_VALUE, "The configuration file you provided is incorrect. One of password or pwd must be filled in. It is not allowed to be empty or blank; it is recommended to fill in password.");
            } else if (StringUtils.isNotEmpty(this.writerConfig.getString(Key.PASSWORD)) && StringUtils.isNotEmpty(this.writerConfig.getString(Key.PWD))) {
                throw DataXException.asDataXException(DolphinDbWriterErrorCode.REQUIRED_VALUE, "The configuration file you provided is incorrect. One of password or pwd must be filled in. It is not allowed to be empty or blank; it is recommended to fill in password.");
            }

            if (StringUtils.isEmpty(this.writerConfig.getString(Key.USER_ID)) && StringUtils.isEmpty(this.writerConfig.getString(Key.USERNAME))) {
                throw DataXException.asDataXException(DolphinDbWriterErrorCode.REQUIRED_VALUE, "The configuration file you provided is wrong, either userId or username must be filled in, empty or blank is not allowed; it is recommended to fill in username first.");
            } else if (StringUtils.isNotEmpty(this.writerConfig.getString(Key.USER_ID)) && StringUtils.isNotEmpty(this.writerConfig.getString(Key.USERNAME))) {
                throw DataXException.asDataXException(DolphinDbWriterErrorCode.REQUIRED_VALUE, "The configuration file you provided is wrong, either userId or username must be filled in, empty or blank is not allowed; it is recommended to fill in username first.");
            }

            if (StringUtils.isEmpty(this.writerConfig.getString(Key.TABLE)) && StringUtils.isEmpty(this.writerConfig.getString(Key.COLUMN))) {
                throw DataXException.asDataXException(DolphinDbWriterErrorCode.REQUIRED_VALUE, "The configuration file you provided is wrong, either table or column must be filled in, empty or blank is not allowed.");
            } else if (StringUtils.isNotEmpty(this.writerConfig.getString(Key.TABLE)) && StringUtils.isNotEmpty(this.writerConfig.getString(Key.COLUMN))) {
                throw DataXException.asDataXException(DolphinDbWriterErrorCode.REQUIRED_VALUE, "The configuration file you provided is wrong, either table or column must be filled in, empty or blank is not allowed.");
            }
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration writerConfig = null;
        private DBConnection dbConnection = null;
        private String functionSql = "";
        private String preSql = "";
        private String postSql = "";
        private String dbName = "";
        private String tbName = "";
        private List<String> colNames_ = null;
        private List<Entity.DATA_TYPE> colTypes_ = null;
        private List<List> colDatas_ = null;

        private List<Integer> extras_ = null;

        private HashMap<Integer, Integer> writeToReadIndexMap;

        private boolean useColumnsParamNotEmpty = false;

        private static final BigDecimal DECIMAL128_MIN_VALUE = new BigDecimal("-170141183460469231731687303715884105728");

        @Override
        public void init() {
            this.writerConfig = super.getPluginJobConf();
            String host = this.writerConfig.getString(Key.HOST);
            int port = this.writerConfig.getInt(Key.PORT);
            String userid = "";
            String configUserId = this.writerConfig.getString(Key.USER_ID);
            String configUsername = this.writerConfig.getString(Key.USERNAME);
            if (StringUtils.isNotEmpty(configUsername) || StringUtils.isNotEmpty(configUserId))
                userid = StringUtils.isNotEmpty(configUsername) ? configUsername : configUserId;

            String pwd = "";
            String configPwd = this.writerConfig.getString(Key.PWD);
            String configPassword = this.writerConfig.getString(Key.PASSWORD);
            if (StringUtils.isNotEmpty(configPassword) || StringUtils.isNotEmpty(configPwd))
                pwd = StringUtils.isNotEmpty(configPassword) ? configPassword : configPwd;

            String saveFunctionDef = this.writerConfig.getString(Key.SAVE_FUNCTION_DEF);
            String saveFunctionName = this.writerConfig.getString(Key.SAVE_FUNCTION_NAME);

            // table:
            List<Object> tableField = this.writerConfig.getList(Key.TABLE);
            JSONArray fieldArr = JSONArray.parseArray(JSON.toJSONString(tableField));
            String dbName = this.writerConfig.getString(Key.DB_PATH);
            String tbName = this.writerConfig.getString(Key.TABLE_NAME);

            // preSql:
            List<Object> preSqlList = this.writerConfig.getList(Key.PRE_SQL);
            if (CollectionUtils.isNotEmpty(preSqlList)) {
                JSONArray preSqlArr = JSONArray.parseArray(JSON.toJSONString(preSqlList));
                List<String> joinPreSqlList = new ArrayList<>();
                for (Object field : preSqlArr) {
                    joinPreSqlList.add(field.toString());
                }
                this.preSql = StringUtils.join(joinPreSqlList, ";");
            }

            // postSql:
            List<Object> postSqlList = this.writerConfig.getList(Key.POST_SQL);
            if (CollectionUtils.isNotEmpty(postSqlList)) {
                JSONArray postSqlArr = JSONArray.parseArray(JSON.toJSONString(postSqlList));
                List<String> joinPostSqlList = new ArrayList<>();
                for (Object field : postSqlArr) {
                    joinPostSqlList.add(field.toString());
                }
                this.postSql = StringUtils.join(joinPostSqlList, ";");
            }

            // taskWriteTimeout:
            Integer taskWriteTimeout = this.writerConfig.getInt(Key.TASK_WRITE_TIMEOUT);

            // column:
            List<Object> columList = this.writerConfig.getList(Key.COLUMN);
            JSONArray columnArr = JSONArray.parseArray(JSON.toJSONString(columList));

            this.dbName = dbName;
            this.tbName = tbName;
            if (this.dbName == null || this.dbName.isEmpty()) {
                this.functionSql = String.format("tableInsert{%s}", tbName);
            } else {
                this.functionSql = String.format("tableInsert{loadTable('%s','%s')}", dbName, tbName);
                if (saveFunctionName != null && !saveFunctionName.isEmpty()) {
                    if (saveFunctionName.equals("upsertTable")){
                        if (saveFunctionDef.contains("keyColNames")){
                            String upsertParameter = saveFunctionDef;
                            saveFunctionDef = DolphinDbTemplate.getTableUpsertScript(userid, pwd, upsertParameter);
                        }
                    }
                    if (saveFunctionDef == null || saveFunctionDef.isEmpty()) {
                        switch (saveFunctionName) {
                            case "savePartitionedData":
                                saveFunctionDef = DolphinDbTemplate.getDfsTableUpdateScript(userid, pwd, fieldArr);
                                break;
                            case "saveDimensionData":
                                saveFunctionDef = DolphinDbTemplate.getDimensionTableUpdateScript(userid, pwd, fieldArr);
                                break;
                        }
                    }
                    this.functionSql = String.format("%s{'%s','%s'}", saveFunctionName, dbName, tbName);
                }
            }

            try {
                dbConnection = new DBConnection();
                DBConnection.ConnectConfig connectConfig;
                connectConfig = DBConnection.ConnectConfig.builder()
                        .hostName(host)
                        .port(port)
                        .userId(userid)
                        .password(pwd)
                        .initialScript(saveFunctionDef)
                        .readTimeout(taskWriteTimeout)
                        .build();

                dbConnection.connect(connectConfig);

                if (columnArr != null) {
                    initColumn(columnArr);
                } else if (fieldArr != null) {
                    initTable(fieldArr);
                }
            } catch (IOException e) {
                LOG.info(saveFunctionDef);
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("Start write to DolphinDB.");
            Record record = null;
            Integer batchSize = this.writerConfig.getInt(Key.BATCH_SIZE);
            if (Objects.isNull(batchSize))
                batchSize = 100000;

            if (!this.preSql.isEmpty())
                executePreSql(this.preSql);

            while ((record = lineReceiver.getFromReader()) != null) {
                parseRecordData(record);
                List firstColumn = colDatas_.get(0);
                if (firstColumn.size() >= batchSize) {
                    BasicTable table = createUploadTable();
                    insertToDolphinDB(table);
                }
            }

            LOG.info("End write to DolphinDB.");
        }

        @Override
        public void post() {
            LOG.info("post() is invoked.");
            insertToDolphinDB(createUploadTable());

            if (!this.postSql.isEmpty())
                executePostSql(this.postSql);
        }

        @Override
        public void destroy() {
            if (dbConnection != null) {
                LOG.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  Close DBConnection !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                dbConnection.close();
            }
        }

        private void insertToDolphinDB(BasicTable bt) {
            LOG.info("Start inserting a table with " + bt.rows() + " rows to DolphinDB.");
            if (bt.rows() == 0)
                return;

            try {
                List<Entity> args = new ArrayList<>();
                args.add(bt);
                dbConnection.run(this.functionSql, args);
            } catch (IOException ex) {
                if (ex.getMessage().contains("Read timed out")) {
                    LOG.error("Failed to insert the table with error: execute current task timeout!");
                    throw new RuntimeException("Failed to insert the table with error: execute current task timeout!", ex);
                } else {
                    LOG.error("Failed to insert the table with error: " + ex.getMessage());
                    throw new RuntimeException(ex.getMessage(), ex);
                }
            }

            LOG.info("End inserting a table to DolphinDB.");
        }

        private void executePreSql(String preSql) {
            LOG.info("execute preSql: " + preSql + " before insertToDolphinDB.");
            try {
                dbConnection.run(preSql);
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        private void executePostSql(String postSql) {
            LOG.info("execute postSql: " + postSql + " after insertToDolphinDB.");
            try {
                dbConnection.run(postSql);
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        private void parseRecordData(Record record) throws RuntimeException {
            int recordLength = record.getColumnNumber();
            Column column;
            if(useColumnsParamNotEmpty && writeToReadIndexMap.size() != recordLength)
                throw new RuntimeException("The columns size of reader isn't equal to the size of writer. ");
            if(!useColumnsParamNotEmpty && colDatas_.size() != recordLength)
                throw new RuntimeException("The columns size of reader isn't equal to the columns size of table to write. ");
            for (int i = 0; i < colDatas_.size(); i++) {
                int readIndex = i;
                if (useColumnsParamNotEmpty) {
                    if (!writeToReadIndexMap.containsKey(i)) {
                        setNullData(i);
                        continue;
                    } else {
                        readIndex = writeToReadIndexMap.get(i);
                    }
                }
                column = record.getColumn(readIndex);
                if (column.getRawData() == null)
                    setNullData(i);
                else
                    setData(i, column);
            }
        }

        private void setNullData(int index){
            List colData = colDatas_.get(index);
            Entity.DATA_TYPE targetType = colTypes_.get(index);
            try {
                switch (targetType) {
                    case DT_DOUBLE:
                        colData.add(-Double.MAX_VALUE);
                        break;
                    case DT_FLOAT:
                        colData.add(-Float.MAX_VALUE);
                        break;
                    case DT_BOOL:
                        colData.add((byte) -128);
                        break;
                    case DT_DATE:
                        colData.add(Integer.MIN_VALUE);
                        break;
                    case DT_MONTH:
                        colData.add(Integer.MIN_VALUE);
                        break;
                    case DT_DATETIME:
                        colData.add(Integer.MIN_VALUE);
                        break;
                    case DT_TIME:
                        colData.add(Integer.MIN_VALUE);
                        break;
                    case DT_SECOND:
                        colData.add(Integer.MIN_VALUE);
                        break;
                    case DT_TIMESTAMP:
                        colData.add(Long.MIN_VALUE);
                        break;
                    case DT_NANOTIME:
                        colData.add(Long.MIN_VALUE);
                        break;
                    case DT_NANOTIMESTAMP:
                        colData.add(Long.MIN_VALUE);
                        break;
                    case DT_LONG:
                        colData.add(Long.MIN_VALUE);
                        break;
                    case DT_INT:
                        colData.add(Integer.MIN_VALUE);
                        break;
                    case DT_UUID:
                        colData.add(new Long2(0, 0));
                        break;
                    case DT_SHORT:
                        colData.add(Short.MIN_VALUE);
                        break;
                    case DT_STRING:
                    case DT_SYMBOL:
                    case DT_BLOB:
                        colData.add("");
                        break;
                    case DT_BYTE:
                        colData.add((byte) -128);
                        break;
                    case DT_DECIMAL32:
                        colData.add(Integer.MIN_VALUE);
                        break;
                    case DT_DECIMAL64:
                        colData.add(Long.MIN_VALUE);
                        break;
                    case DT_DECIMAL128:
                        colData.add(DECIMAL128_MIN_VALUE);
                        break;
                    default:
                        throw new RuntimeException(Utils.getDataTypeString(targetType) + "is not supported. ");
                }
            } catch (Exception e) {
                LOG.error("Failed to set NULL to column '" + colNames_.get(index) + "'.");
            }
        }

        private void setData(int index, Column column) {
            List colData = colDatas_.get(index);
            Entity.DATA_TYPE targetType = colTypes_.get(index);
            try {
                switch (targetType) {
                    case DT_DOUBLE:
                        colData.add(column.asDouble());
                        break;
                    case DT_FLOAT:
                        colData.add(Float.parseFloat(column.asString()));
                        break;
                    case DT_BOOL:
                        colData.add(column.asBoolean() ? (byte) 1 : (byte) 0);
                        break;
                    case DT_DATE:
                        colData.add(Utils.countDays(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate()));
                        break;
                    case DT_MONTH:
                        LocalDate d = column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                        colData.add(Utils.countMonths(d.getYear(), d.getMonthValue()));
                        break;
                    case DT_DATETIME:
                        colData.add(Utils.countSeconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                        break;
                    case DT_TIME:
                        colData.add(Utils.countMilliseconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalTime()));
                        break;
                    case DT_SECOND:
                        colData.add(Utils.countSeconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalTime()));
                        break;
                    case DT_TIMESTAMP:
                        colData.add(Utils.countMilliseconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                        break;
                    case DT_NANOTIME:
                        colData.add(Utils.countNanoseconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalTime()));
                        break;
                    case DT_NANOTIMESTAMP:
                        colData.add(Utils.countNanoseconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                        break;
                    case DT_LONG:
                        colData.add(column.asLong());
                        break;
                    case DT_INT:
                        colData.add(Integer.parseInt(column.asString()));
                        break;
                    case DT_UUID:
                        colData.add(BasicUuid.fromString(column.asString().trim()).getLong2());
                        break;
                    case DT_SHORT:
                        colData.add(Short.parseShort(column.asString()));
                        break;
                    case DT_STRING:
                    case DT_SYMBOL:
                    case DT_BLOB:
                        colData.add(column.asString());
                        break;
                    case DT_BYTE:
                        colData.add(column.asBytes());
                        break;
                    case DT_DECIMAL32:
                        colData.add(column.asString());
                        break;
                    case DT_DECIMAL64:
                        colData.add(column.asString());
                        break;
                    case DT_DECIMAL128:
                        colData.add(column.asString());
                        break;
                    default:
                        throw new RuntimeException(Utils.getDataTypeString(targetType) + "is not supported. ");
                }
            } catch (Exception ex){
                LOG.error("Failed to parse the column '" + colNames_.get(index) + "': " + column.toString() + ".");
                throw ex;
            }
        }

        private List<?> getListFromColumn(Entity.DATA_TYPE targetType) {
            List<?> vec = null;
            switch (targetType) {
                case DT_DOUBLE:
                    vec = new ArrayList<Double>();
                    break;
                case DT_FLOAT:
                    vec = new ArrayList<Float>();
                    break;
                case DT_BOOL:
                    vec = new ArrayList<Boolean>();
                    break;
                case DT_DATE:
                case DT_MONTH:
                case DT_DATETIME:
                case DT_TIME:
                case DT_SECOND:
                case DT_INT:
                    vec = new ArrayList<Integer>();
                    break;
                case DT_TIMESTAMP:
                case DT_NANOTIME:
                case DT_NANOTIMESTAMP:
                case DT_LONG:
                    vec = new ArrayList<Long>();
                    break;
                case DT_UUID:
                    vec = new ArrayList<Long2>();
                    break;
                case DT_SHORT:
                    vec = new ArrayList<Short>();
                    break;
                case DT_STRING:
                case DT_SYMBOL:
                case DT_BLOB:
                    vec = new ArrayList<String>();
                    break;
                case DT_BYTE:
                    vec = new ArrayList<Byte>();
                    break;
                case DT_DECIMAL32:
                case DT_DECIMAL64:
                case DT_DECIMAL128:
                    vec = new ArrayList<BigDecimal>();
                    break;
            }
            if (vec == null) LOG.info(targetType.toString() + " get Vec is NULL!!!!!");
            return vec;
        }

        private BasicTable createUploadTable() {
            List<Vector> columns = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();
            for (int i = 0; i < colNames_.size(); i++){
                Vector vec = generateDDBTableColumn(colDatas_.get(i), colTypes_.get(i), extras_.get(i), i);
                columns.add(vec);
                columnNames.add(colNames_.get(i));
            }

            for (int i = 0; i < colNames_.size(); i++)
                colDatas_.get(i).clear();

            return new BasicTable(columnNames, columns);
        }

        private Vector generateDDBTableColumn(List colData, Entity.DATA_TYPE targetType, int extra, int column) {
            Vector vec = null;
            String currentDecimalValue = null;
            try {
                switch (targetType) {
                    case DT_DOUBLE:
                        vec = new BasicDoubleVector(colData);
                        break;
                    case DT_FLOAT:
                        vec = new BasicFloatVector(colData);
                        break;
                    case DT_BOOL:
                        vec = new BasicBooleanVector(colData);
                        break;
                    case DT_DATE:
                        vec = new BasicDateVector(colData);
                        break;
                    case DT_MONTH:
                        vec = new BasicMonthVector(colData);
                        break;
                    case DT_DATETIME:
                        vec = new BasicDateTimeVector(colData);
                        break;
                    case DT_TIME:
                        vec = new BasicTimeVector(colData);
                        break;
                    case DT_SECOND:
                        vec = new BasicSecondVector(colData);
                        break;
                    case DT_TIMESTAMP:
                        vec = new BasicTimestampVector(colData);
                        break;
                    case DT_NANOTIME:
                        vec = new BasicNanoTimeVector(colData);
                        break;
                    case DT_NANOTIMESTAMP:
                        vec = new BasicNanoTimestampVector(colData);
                        break;
                    case DT_LONG:
                        vec = new BasicLongVector(colData);
                        break;
                    case DT_INT:
                        vec = new BasicIntVector(colData);
                        break;
                    case DT_UUID:
                        vec = new BasicUuidVector(colData);
                        break;
                    case DT_SHORT:
                        vec = new BasicShortVector(colData);
                        break;
                    case DT_STRING:
                    case DT_SYMBOL:
                        List<String> stringList = new ArrayList<>();
                        for (int i = 0; i < colData.size() ; i++) {
                            byte[] bytes = ((String) colData.get(i)).getBytes();
                            if (bytes.length > 65535) {
                                stringList.add(((String) colData.get(i)).substring(0, 21845));
                                LOG.warn("Size of STRING can’t exceed 65535 byte. Cut the String length to 21845. Record size: {}, Column name: {}, Row index: {}.",bytes.length,colNames_.get(column),i);
                            } else {
                                stringList.add((String) colData.get(i));
                            }
                        }
                        vec = new BasicStringVector(stringList);
                        break;
                    case DT_BLOB:
                        List<String> bolbList = new ArrayList<>();
                        for (int i = 0; i < colData.size() ; i++) {
                            byte[] bytes = ((String) colData.get(i)).getBytes();
                            if (bytes.length > 4194304) {
                                bolbList.add(((String) colData.get(i)).substring(0, 1398101));
                                LOG.warn("Size of BLOB can’t exceed 4194304 byte. Cut the blob length to 1398101. Record size: {}, Column name: {}, Row index: {}.",bytes.length,colNames_.get(column),i);
                            } else {
                                bolbList.add((String) colData.get(i));
                            }
                        }
                        vec = new BasicStringVector(bolbList,true);
                        break;
                    case DT_BYTE:
                        vec = new BasicByteVector(colData);
                        break;
                    case DT_DECIMAL32: {
                        int scale = extra;
                        vec = new BasicDecimal32Vector(colData.size(), scale);
                        for (int i = 0; i < colData.size(); i++) {
                            BasicDecimal32 scalar;
                            if (colData.get(i).equals(Integer.MIN_VALUE)) {
                                scalar = new BasicDecimal32("0", 0);
                                scalar.setNull();
                            } else {
                                String s = (String) colData.get(i);
                                currentDecimalValue = s;
                                scalar = new BasicDecimal32(s, scale);
                            }

                            try {
                                vec.set(i, scalar);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                        break;
                    }
                    case DT_DECIMAL64: {
                        int scale = extra;
                        vec = new BasicDecimal64Vector(colData.size(), scale);
                        for (int i = 0; i < colData.size(); i++) {
                            BasicDecimal64 scalar;
                            if (colData.get(i).equals(Long.MIN_VALUE)) {
                                scalar = new BasicDecimal64("0", 0);
                                scalar.setNull();
                            } else {
                                String s = (String) colData.get(i);
                                currentDecimalValue = s;
                                scalar = new BasicDecimal64(s, scale);
                            }

                            try {
                                vec.set(i, scalar);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                        break;
                    }
                    case DT_DECIMAL128:{
                        int scale = extra;
                        vec = new BasicDecimal128Vector(colData.size(), scale);
                        for (int i = 0; i < colData.size(); i++) {
                            BasicDecimal128 scalar;
                            if (colData.get(i).equals(DECIMAL128_MIN_VALUE)) {
                                scalar = new BasicDecimal128("0", 0);
                                scalar.setNull();
                            } else {
                                String s = (String) colData.get(i);
                                currentDecimalValue = s;
                                scalar = new BasicDecimal128(s, scale);
                            }

                            try {
                                vec.set(i, scalar);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                StringBuilder errorMsg = new StringBuilder();
                errorMsg.append("Error generating the table to be written to DolphinDB, error column '")
                        .append(colNames_.get(column))
                        .append("': ");
                if (targetType == Entity.DATA_TYPE.DT_DECIMAL32 || targetType == Entity.DATA_TYPE.DT_DECIMAL64 || targetType == Entity.DATA_TYPE.DT_DECIMAL128)
                    errorMsg.append(currentDecimalValue).append(".");
                else
                    errorMsg.append(colData).append(".");

                LOG.error(errorMsg.toString());
                throw new RuntimeException(errorMsg.toString());
            }

            return vec;
        }

        private void initTable(JSONArray fieldArr) {
            this.colNames_ = new ArrayList<>();
            this.colDatas_ = new ArrayList<>();
            this.colTypes_ = new ArrayList<>();
            this.extras_ = new ArrayList<>();
            if (fieldArr.toString().equals("[]")){
                try {
                    BasicDictionary schema;
                    if (this.dbName == null || dbName.isEmpty()){
                        schema = (BasicDictionary) dbConnection.run(tbName + ".schema()");
                    }else {
                        schema = (BasicDictionary) dbConnection.run("loadTable(\"" + dbName + "\"" + ",`" + tbName + ").schema()");
                    }
                    BasicTable colDefs = (BasicTable)schema.get(new BasicString("colDefs"));
                    BasicStringVector colNames = (BasicStringVector) colDefs.getColumn("name");
                    BasicIntVector colTypeInt = (BasicIntVector) colDefs.getColumn("typeInt");
                    BasicIntVector extraInt = (BasicIntVector) colDefs.getColumn("extra");
                    for (int i = 0; i < colDefs.rows(); i++){
                        Entity.DATA_TYPE type = Entity.DATA_TYPE.valueOf(colTypeInt.getInt(i));
                        String colName = colNames.getString(i);
                        List colData = getListFromColumn(type);
                        this.colNames_.add(colName);
                        this.colDatas_.add(colData);
                        this.colTypes_.add(type);
                        this.extras_.add(extraInt.getInt(i));
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            } else {
                for (int i = 0; i < fieldArr.size(); i++) {
                    try {
                        JSONObject field = fieldArr.getJSONObject(i);
                        String colName = field.getString("name");
                        Entity.DATA_TYPE type = Entity.DATA_TYPE.valueOf(field.getString("type"));

                        BasicDictionary schema;
                        if (this.dbName == null || dbName.isEmpty()){
                            schema = (BasicDictionary) dbConnection.run(tbName + ".schema()");
                        }else {
                            schema = (BasicDictionary) dbConnection.run("loadTable(\"" + dbName + "\"" + ",`" + tbName + ").schema()");
                        }
                        BasicTable colDefs = (BasicTable)schema.get(new BasicString("colDefs"));
                        BasicIntVector extraInt = (BasicIntVector) colDefs.getColumn("extra");

                        List colData = getListFromColumn(type);
                        this.colNames_.add(colName);
                        this.colDatas_.add(colData);
                        this.colTypes_.add(type);
                        this.extras_.add(extraInt.getInt(i));
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }
        }

        private void initColumn(JSONArray fieldArr) {
            this.colNames_ = new ArrayList<>();
            this.colDatas_ = new ArrayList<>();
            this.colTypes_ = new ArrayList<>();
            this.writeToReadIndexMap = new HashMap<>();
            this.useColumnsParamNotEmpty = false;
            this.extras_ = new ArrayList<>();

            if (fieldArr.toString().equals("[\"*\"]")){
                try {
                    BasicDictionary schema;
                    if (this.dbName == null || dbName.isEmpty()){
                        schema = (BasicDictionary) dbConnection.run(tbName + ".schema()");
                    }else {
                        schema = (BasicDictionary) dbConnection.run("loadTable(\"" + dbName + "\"" + ",`" + tbName + ").schema()");
                    }
                    BasicTable colDefs = (BasicTable)schema.get(new BasicString("colDefs"));
                    BasicStringVector colNames = (BasicStringVector) colDefs.getColumn("name");
                    BasicIntVector colTypeInt = (BasicIntVector) colDefs.getColumn("typeInt");
                    BasicIntVector extraInt = (BasicIntVector) colDefs.getColumn("extra");
                    for (int i = 0; i < colDefs.rows(); i++){
                        Entity.DATA_TYPE type = Entity.DATA_TYPE.valueOf(colTypeInt.getInt(i));
                        String colName = colNames.getString(i);
                        List colData = getListFromColumn(type);
                        this.colNames_.add(colName);
                        this.colDatas_.add(colData);
                        this.colTypes_.add(type);
                        this.extras_.add(extraInt.getInt(i));
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            } else {
                this.useColumnsParamNotEmpty = true;
                try {
                    BasicDictionary schema;
                    if (this.dbName == null || dbName.isEmpty()) {
                        schema = (BasicDictionary) dbConnection.run(tbName + ".schema()");
                    } else {
                        schema = (BasicDictionary) dbConnection.run("loadTable(\"" + dbName + "\"" + ",`" + tbName + ").schema()");
                    }
                    BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
                    BasicStringVector writerColNames = (BasicStringVector) colDefs.getColumn("name");
                    BasicIntVector colTypeInt = (BasicIntVector) colDefs.getColumn("typeInt");
                    BasicIntVector extraInt = (BasicIntVector) colDefs.getColumn("extra");

                    //get read colname list, check colname whether have same
                    HashSet<String> readColNamesSet = new HashSet<>();
                    List<String> readColNames = new ArrayList<>();
                    for (Object field : fieldArr) {
                        String colName = field.toString();
                        if (readColNamesSet.contains(colName))
                            throw new Exception("colName can be same in columns parameter. ");
                        readColNamesSet.add(colName);
                        readColNames.add(colName);
                    }

                    // create map(writeColName, writeColumnIndx)
                    HashMap<String, Integer> writeColNamesIndex = new HashMap<String, Integer>();
                    for (int i = 0; i < writerColNames.rows(); i++){
                        String colName = writerColNames.getString(i);
                        if(writeColNamesIndex.containsKey(colName))
                            throw new Exception("colName can be same in table to write.");
                        writeColNamesIndex.put(colName, i);
                    }

                    // create map(writeColumnIndex, readColumnIndex)
                    // the size of param columns must be not less than the columns of table to write
                    // the size of param columns must be equal to size of reader columns, check when recordToBasicTable
                    for(int i = 0; i < readColNames.size(); ++i){
                        if(!writeColNamesIndex.containsKey(readColNames.get(i)))
                            throw new Exception("The " + readColNames.get(i) + " column in columns parameter does not exist in table to write");
                        int writeIndex =  writeColNamesIndex.get(readColNames.get(i));
                        writeToReadIndexMap.put(writeIndex, i);
                    }

                    for(int i = 0; i < writerColNames.rows(); ++i){
                        this.colNames_.add(writerColNames.getString(i));
                        Entity.DATA_TYPE type = Entity.DATA_TYPE.valueOf(colTypeInt.getInt(i));
                        List colData = getListFromColumn(type);
                        this.colDatas_.add(colData);
                        this.colTypes_.add(type);
                        this.extras_.add(extraInt.getInt(i));
                    }

                }catch (Exception e){
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }
}
