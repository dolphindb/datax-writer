package com.alibaba.datax.plugin.writer.dolphindbwriter;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class DolphinDbTemplate {
    private static final String _dimensionTableUpdateScript = "def saveDimensionData(dbName, tbName, data){\n" +
            "                          login('admin','123456')\n" +
            "                          dfsPath = 'dfs://' + dbName\n" +
            "                          temp = select * from loadTable(dbPath, tbName)\n" +
            "                          [UPDATESQL]\n" +
            "                          db = database(dbPath)\n" +
            "                          db.dropTable(tbName)\n" +
            "                          dt = db.createTable(temp, tbName)\n" +
            "                          dt.append!(temp)}";
    private static final String _dfsTableUpdateScript = "def rowUpdate(dbName, tbName, data, t){\n" +
            "updateRowCount = exec count(*) from ej(tbB,tbA,[KEYFIELD])\n" +
            "if(updateRowCount<=0) return\n" +
            "dfsPath = 'dfs://' + dbName\n" +
            "temp = select * from t\n" +
            "cp = t.schema().chunkPath.substr(strlen(\"/\" + dbName))\n" +
            "dropPartition(database(dfsPath), cp, tbName)\n" +
            "[UPDATESQL]\n" +
            "loadTable(dfsPath, tbName).append!(temp)\n" +
            "}\n" +
            "def savePartitionedData(dbName, tbName, data){\n" +
            "dfsPath = 'dfs://' + dbName\n" +
            "login('admin','123456')\n" +
            "t = loadTable(dfsPath, tbName)\n" +
            "ds1 = sqlDS(<select * from t>)\n" +
            "mr(ds1, rowUpdate{dbName, tbName, data})\n" +
            "}";

    public static String getDimensionTableUpdateScript(JSONArray fieldArr){
        String updateSql = "update temp set ";
        String whereSql = " from  ej(temp, data, %s) ";
        String keyName = "";
        for (int i = 0; i < fieldArr.size(); i++) {
            JSONObject field = fieldArr.getJSONObject(i);
            String colName = field.getString("name");
            boolean isKeyField = field.containsKey("isKeyField")?field.getBoolean("isKeyField"):false;
            if(isKeyField){
                keyName += "`" + colName;
            }else{
                updateSql += colName + "= data." + colName + ", ";
            }
        }
        whereSql = String.format(whereSql, keyName);
        updateSql = updateSql.substring(0,updateSql.length()-2);
        updateSql = updateSql + whereSql;
        return _dimensionTableUpdateScript.replace("[UPDATESQL]",updateSql);
    }

    public static String getDfsTableUpdateScript(JSONArray fieldArr){
        String updateSql = "update temp set ";
        String whereSql = " from  ej(temp, data, %s) ";
        String keyName = "";
        for (int i = 0; i < fieldArr.size(); i++) {
            JSONObject field = fieldArr.getJSONObject(i);
            String colName = field.getString("name");
            boolean isKeyField = field.containsKey("isKeyField")?field.getBoolean("isKeyField"):false;
            if(isKeyField){
                keyName += "`" + colName;
            }else{
                updateSql += colName + "= data." + colName + ", ";
            }
        }
        whereSql = String.format(whereSql, keyName);
        updateSql = updateSql.substring(0,updateSql.length()-2);
        updateSql = updateSql + whereSql;
        return _dfsTableUpdateScript.replace("[UPDATESQL]",updateSql).replace("[KEYFIELD]", keyName);
    }
}
