/**
 * Impelement of job and task
 *
 * @author DolphinDB
 * @author Apple
 */
package com.alibaba.datax.plugin.writer.dolphindbwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.io.Long2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DolphinDbWriter extends Writer {

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
            LOG.info("dolphindbwriter params:{}", this.writerConfig.toJSON());
        }

        @Override
        public void destroy() {

        }

        /**
         *
         */
        private void validateParameter() {
            this.writerConfig.getNecessaryValue(Key.HOST, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            this.writerConfig.getNecessaryValue(Key.PORT, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            this.writerConfig.getNecessaryValue(Key.PWD, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            this.writerConfig.getNecessaryValue(Key.USER_ID, DolphinDbWriterErrorCode.REQUIRED_VALUE);
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration writerConfig = null;
        private DBConnection dbConnection = null;
        private String functionSql = "";
        private String dbName = "";
        private String tbName = "";
        private List<String> colNames_ = null;
        private List<Entity.DATA_TYPE> colTypes_ = null;
        private List<List> colDatas_ = null;

        @Override
        public void startWrite(RecordReceiver lineReceiver) {

            LOG.info("start to writer DolphinDB");
            Record record = null;
            List<Object> tableField = this.writerConfig.getList(Key.TABLE);
            JSONArray fieldArr = JSONArray.parseArray(JSON.toJSONString(tableField));

            Integer batchSize = this.writerConfig.getInt(Key.BATCH_SIZE);
            if(batchSize==null){
                batchSize = 10000000;
            }
            while ((record = lineReceiver.getFromReader()) != null) {
                recordToBasicTable(record);
                List firstColumn = colDatas_.get(0);
                if (firstColumn.size() >= batchSize) {
                    insertToDolphinDB(createUploadTable());
                    initColumn(fieldArr);
                }
            }
        }

        private void insertToDolphinDB(BasicTable bt) {
            LOG.info("begin to write BasicTable rows = " + String.valueOf(bt.rows()));
            List<Entity> args = new ArrayList<>();
            args.add(bt);
            try {
                dbConnection.run(this.functionSql, args);
                LOG.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!    " + args.get(0).getString());
                LOG.info("end write BasicTable");
            } catch (IOException ex) {
                LOG.error(ex.getMessage());
            }
        }

        private void recordToBasicTable(Record record) {
            int recordLength = record.getColumnNumber();
            Column column;
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                setData(i, column);
            }
        }

        private void setData(int index, Column column) {
            List colData = colDatas_.get(index);
            Entity.DATA_TYPE targetType = colTypes_.get(index);
            try {
                switch (targetType) {
                    case DT_DOUBLE:
                        if (column.getRawData() == null)
                            colData.add(-Double.MAX_VALUE);
                        else
                            colData.add(column.asDouble());
                        break;
                    case DT_FLOAT:
                        if (column.getRawData() == null)
                            colData.add(-Float.MAX_VALUE);
                        else
                            colData.add(Float.parseFloat(column.asString()));
                        break;
                    case DT_BOOL:
                        if (column.getRawData() == null)
                            colData.add((byte) -128);
                        else
                            colData.add(column.asBoolean() == true ? (byte) 1 : (byte) 0);
                        break;
                    case DT_DATE:
                        if (column.getRawData() == null)
                            colData.add(Integer.MIN_VALUE);
                        else
                            colData.add(Utils.countDays(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate()));
                        break;
                    case DT_MONTH:
                        if (column.getRawData() == null)
                            colData.add(Integer.MIN_VALUE);
                        else {
                            LocalDate d = column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                            colData.add(Utils.countMonths(d.getYear(), d.getMonthValue()));
                        }
                        break;
                    case DT_DATETIME:
                        if (column.getRawData() == null)
                            colData.add(Integer.MIN_VALUE);
                        else
                            colData.add(Utils.countSeconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                        break;
                    case DT_TIME:
                        if (column.getRawData() == null)
                            colData.add(Integer.MIN_VALUE);
                        else
                            colData.add(Utils.countMilliseconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalTime()));
                        break;
                    case DT_SECOND:
                        if (column.getRawData() == null)
                            colData.add(Integer.MIN_VALUE);
                        else
                            colData.add(Utils.countSeconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalTime()));
                        break;
                    case DT_TIMESTAMP:
                        if (column.getRawData() == null)
                            colData.add(Long.MIN_VALUE);
                        else
                            colData.add(Utils.countMilliseconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                        break;
                    case DT_NANOTIME:
                        if (column.getRawData() == null)
                            colData.add(Long.MIN_VALUE);
                        else
                            colData.add(Utils.countNanoseconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalTime()));
                        break;
                    case DT_NANOTIMESTAMP:
                        if (column.getRawData() == null)
                            colData.add(Long.MIN_VALUE);
                        else
                            colData.add(Utils.countNanoseconds(column.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                        break;
                    case DT_LONG:
                        if (column.getRawData() == null)
                            colData.add(Long.MIN_VALUE);
                        else
                            colData.add(column.asLong());
                        break;
                    case DT_INT:
                        if (column.getRawData() == null)
                            colData.add(Integer.MIN_VALUE);
                        else
                            colData.add(Integer.parseInt(column.asString()));
                        break;
                    case DT_UUID:
                        if (column.getRawData() == null)
                            colData.add(new Long2(0, 0));
                        else
                            colData.add(BasicUuid.fromString(column.asString().trim()).getLong2());
                        break;
                    case DT_SHORT:
                        if (column.getRawData() == null)
                            colData.add(Short.MIN_VALUE);
                        else
                            colData.add(Short.parseShort(column.asString()));
                        break;
                    case DT_STRING:
                    case DT_SYMBOL:
                        if (column.getRawData() == null)
                            colData.add("");
                        else
                            colData.add(column.asString());
                        break;
                    case DT_BYTE:
                        if (column.getRawData() == null)
                            colData.add((byte) -128);
                        else
                            colData.add(column.asBytes());
                        break;
                }
            }catch (Exception ex){
                LOG.info("Parse error : colName=" + colNames_.get(index));
                throw ex;
            }
        }

        private List getListFromColumn(Entity.DATA_TYPE targetType) {
            List vec = null;
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
                    vec = new ArrayList<String>();
                    break;
                case DT_BYTE:
                    vec = new ArrayList<Byte>();
                    break;
            }
            if (vec == null) LOG.info(targetType.toString() + " get Vec is NULL!!!!!");
            return vec;
        }

        private BasicTable createUploadTable() {
            List<Vector> columns = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();
            for (int i = 0; i < colNames_.size(); i++){
                columns.add(getDDBColFromColumn(colDatas_.get(i), colTypes_.get(i)));
                columnNames.add(colNames_.get(i));
            }
            return new BasicTable(columnNames, columns);
        }

        private Vector getDDBColFromColumn(List colData, Entity.DATA_TYPE targetType) {
            Vector vec = null;
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
                    vec = new BasicStringVector(colData);
                    break;
                case DT_BYTE:
                    vec = new BasicByteVector(colData);
            }
            return vec;
        }

        private void initColumn(JSONArray fieldArr) {
            this.colNames_ = new ArrayList<>();
            this.colDatas_ = new ArrayList<>();
            this.colTypes_ = new ArrayList<>();
            if (fieldArr.toString().equals("[]")){
                try {
                    BasicDictionary schema;
                    if (this.dbName == null || dbName.equals("")){
                        schema = (BasicDictionary) dbConnection.run(tbName + ".schema()");
                    }else {
                        schema = (BasicDictionary) dbConnection.run("loadTable(\"" + dbName + "\"" + ",`" + tbName + ").schema()");
                    }
                    BasicTable colDefs = (BasicTable)schema.get(new BasicString("colDefs"));
                    BasicStringVector colNames = (BasicStringVector) colDefs.getColumn("name");
                    BasicIntVector colTypeInt = (BasicIntVector) colDefs.getColumn("typeInt");
                    for (int i = 0; i < colDefs.rows(); i++){
                        Entity.DATA_TYPE type = Entity.DATA_TYPE.valueOf(colTypeInt.getInt(i));
                        String colName = colNames.getString(i);
                        List colData = getListFromColumn(type);
                        this.colNames_.add(colName);
                        this.colDatas_.add(colData);
                        this.colTypes_.add(type);
                    }
                }catch (Exception e){
                    LOG.error(e.getMessage(),e);
                }
            }else {
                for (int i = 0; i < fieldArr.size(); i++) {
                    JSONObject field = fieldArr.getJSONObject(i);
                    String colName = field.getString("name");
                    Entity.DATA_TYPE type = Entity.DATA_TYPE.valueOf(field.getString("type"));
                    List colData = getListFromColumn(type);
                    this.colNames_.add(colName);
                    this.colDatas_.add(colData);
                    this.colTypes_.add(type);
                }
            }
        }

        @Override
        public void init() {

            this.writerConfig = super.getPluginJobConf();
            String host = this.writerConfig.getString(Key.HOST);
            int port = this.writerConfig.getInt(Key.PORT);
            String userid = this.writerConfig.getString(Key.USER_ID);
            String pwd = this.writerConfig.getString(Key.PWD);

            String saveFunctionDef = this.writerConfig.getString(Key.SAVE_FUNCTION_DEF);
            String saveFunctionName = this.writerConfig.getString(Key.SAVE_FUNCTION_NAME);
            List<Object> tableField = this.writerConfig.getList(Key.TABLE);
            JSONArray fieldArr = JSONArray.parseArray(JSON.toJSONString(tableField));
            String dbName = this.writerConfig.getString(Key.DB_PATH);
            String tbName = this.writerConfig.getString(Key.TABLE_NAME);
            this.dbName = dbName;
            this.tbName = tbName;
            if(this.dbName == null || this.dbName.equals("")){
                this.functionSql = String.format("tableInsert{%s}", tbName);
            }else {
                this.functionSql = String.format("tableInsert{loadTable('%s','%s')}", dbName, tbName);
                if (saveFunctionName != null && !saveFunctionName.equals("")) {
                    if (saveFunctionName.equals("upsertTable")){
                        if (saveFunctionDef.contains("keyColNames")){
                            String upsertParameter = saveFunctionDef;
                            saveFunctionDef = DolphinDbTemplate.getTableUpsertScript(userid, pwd, upsertParameter);
                        }
                    }
                    if (saveFunctionDef == null || saveFunctionDef.equals("")) {
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
            dbConnection = new DBConnection();
            try {
                if (saveFunctionDef == null || saveFunctionDef.equals("")) {
                    dbConnection.connect(host, port, userid, pwd);
                } else {
                    LOG.info(saveFunctionDef);
                    dbConnection.connect(host, port, userid, pwd, saveFunctionDef);
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(),e);
                LOG.info(saveFunctionDef);
            }
            initColumn(fieldArr);
        }

        @Override
        public void post() {
            LOG.info("post is invoked");
            insertToDolphinDB(createUploadTable());
        }

        @Override
        public void destroy() {

            if (dbConnection != null) {
                dbConnection.close();
            }
        }
    }
}
