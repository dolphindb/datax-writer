# 基于DataX的DolphinDB数据导入工具
## 1. 使用场景
DataX-dolphindbwriter插件是解决用户将不同数据来源的数据同步到DolphinDB的场景而开发的，这些数据的特征是改动很少, 并且数据分散在不同的数据库系统中。

## 2. DataX离线数据同步
DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、DRDS 等各种异构数据源之间高效的数据同步功能, [DataX已支持的数据源](https://github.com/alibaba/DataX/blob/master/README.md#support-data-channels)。

DataX是可扩展的数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件。理论上DataX框架可以支持任意数据源类型的数据同步工作。每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。


#### DataX插件 ：dolphindbwriter
<<<<<<< HEAD
基于DataX的扩展功能，dolphindbwriter插件实现了向DolphinDB写入数据，使用DataX的现有reader插件结合DolphinDBWriter插件，即可满足从不同数据源向DolphinDB同步数据。

DolphinDBWriter底层依赖于 DolphinDB Java API，采用批量写入的方式，将数据写入分布式数据库。
=======
基于DataX的扩展功能，dolphindbwriter插件实现了向DolphinDB写入数据，使用DataX的现有reader插件结合DolphinDBWriter插件，即可满足从不同数据源向DolphinDB同步数据的场景。
DolphinDBWriter底层依赖于 DolphinDB Java API，每次达到10000条记录时写入一次DolphinDB,最后不满10000条的数据,会在结束事件(POST)触发时写入。
>>>>>>> 2dff81063a0a52acfd9e3911af4234aebc743f0e

## 3. 使用方法
详细信息请参阅 [DataX指南](https://github.com/alibaba/DataX/blob/master/userGuid.md), 以下仅列出必要步骤。

### 3.1 下载部署DataX

Download [DataX下载地址](http://datax-opensource.oss-cn-hangzhou.aliyuncs.com/datax.tar.gz)

### 3.2 部署DataX-DolphinDBWriter插件

将 [dolphindbwriter](http://www.dolphindb.com/downloads/dolphindbwriter.zip) 整个目录拷贝到plugin目录下，即可以使用。

### 3.3 执行DataX任务

进入datax/bin目录下，用 python 执行 datax.py 脚本，并指定配置文件地址，示例如下：
```
cd /root/datax/bin/
python datax.py /root/datax/myconf/BASECODE.json
```

### 3.4 导入实例
使用DataX绝大部分工作都是通过配置来完成，包括双边的数据库连接信息和需要同步的数据表结构信息等。

#### 3.4.1 全量导入
下面以从oracle向DolphinDB导入一张表BASECODE来举个例子.

使用oraclereader从oracle读取BASECODE表读取全量数据，dolphindbwriter将读取的BASECODE数据写入DolphinDB中。

首先的工作需要编写配置文件BASECODE.json，存放到指定目录，比如 /root/datax/myconf目录下，配置文件说明参考附录。

配置完成后，在datax/bin目录下执行如下脚本即可启动同步任务
```
cd /root/datax/bin/
python datax.py /root/datax/myconf/BASECODE.json
```
#### 3.4.2 增量数据导入
增量数据分两种类型，一种是新增数据，另一种是已有数据的更新，即更新了数据内容以及时间戳。对于这两种类型，要用不同的数据导入方式。

* 新增数据增量同步

    新增数据的增量同步，与全量导入相比，唯一的不同点在于reader中对数据源的对已导入数据过滤。通常需要处理增量数据的表，都会有一个时间戳的列来标记入库时间，在oralce reader插件中，只需要配置where条件，增加时间戳过滤即可,对于dolphindbwriter的配置与全量导入完全相同。比如时间戳字段为OPDATE, 要增量导入2020.03.01之后的增量数据，那么配置
 `"where": "OPDATE > to_date('2020-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')`
 
 * 变更数据增量同步
    
    变更数据在数据源有不同的记录方法，比较规范的方法是通过一个变更标志和时间戳来记录，比如用OPTYPE、 OPDATE来记录变更的类型和时间戳，这样可以通过类似 `"where": "OPTYPE=1 and OPDATE > to_date('2020-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')` 条件过滤出增量数据。
    对于writer的配置项，需要增加如下两处配置:
    * isKeyField        
        
        因为变更数据的更新需要目标表中有唯一列，所以writer的配置中，需要对table配置项中唯一键列增加isKeyField=true这一配置项。

    * saveFunctionName
    
        DolphinDB有多种数据落盘的方式，比较常用的两种是分布式表和维度表。dolphindbwriter中内置了更新这两种表的脚本模板，当从数据源中过滤出变更数据之后，在writer配置中增加`saveFunctionName`和`saveFunctionDef`两个配置(具体用法请参考附录)，writer会根据这两个配置项，采用对应的方式将数据更新到DolphinDB中。

    
    当有些数据源中不包含OPTYPE这一标识列，无法分辨出新数据是更新或是新增的时候，可以作为新增数据入库，函数视图输出的方式：

    * 数据作为新增数据处理。这种方式处理后，数据表中存在重复键值。
    
    * 定义functionView作为数据访问接口，在functionView中对有重复键值的数据仅输出时间戳最新的一条。
    
    * 用户不能直接访问表(可以取消非管理员用户访问表的权限)，统一通过functionView访问数据。

* 定时同步

    * 利用shell脚本实现DataX定时增量数据同步

    DataX从设计上用于离线数据的一次性同步场景，我们可以通过shell或python脚本等工程方式实现定时增量同步。

    由于 DataX 支持非常灵活的配置， 一种相对简单并且可靠的思路就是根据时间戳动态修改配置文件：

        利用 DataX 的 reader 去目标数据库读取数据，并记录最新时间戳。
        将这个最新时间戳写入到一个json文本文件(op_table.json)；
        再次执行同步时用脚本来读取json文本文件， 并动态修改同步的配置文件；
        执行修改后的配置文件(run_job.json)，进行增量同步。

    本脚本默认约定，每一个数据表中都有OPDATE和OPMODE两个字段，用于标记操作时间和操作方式。OPMODE=0为新增数据，OPMODE=1，2为更新和删除数据。在运行时，根据保存的时间戳动态为oraclereader增加一个where配置项：
    ```
    OPMODE = 0 AND OPDATE > to_date('[最新时间戳]', 'YYYY-MM-DD HH24:MI:SS') //新增数据
    
    OPMODE > 0 AND OPDATE > to_date('[最新时间戳]', 'YYYY-MM-DD HH24:MI:SS') //更新数据
    ```

定时同步是通过python脚本实现，它在datax的发布包中包含。
增量同步脚本在根目录ddb_script下，下载后保存在本地磁盘比如/root/ddb_script/。

假设datax根目录为/root/datax, 配置文件放在/root/datax/myconf/目录下，则增量同步的使用方法为
```
cd /root/ddb_script/
python main.py /root/datax/bin/datax.py /root/datax/myconf/BASECODE.json [run_type]
```

run_type 参数为选项值，当前支持 [test|prod], 具体说明如下：
```
设置为test时，脚本实时打印 datax 输出的内容，
此设置下不会向op_table.json文件更新时间戳，可供重复调试配置文件使用。

设置为prod时，执行后会更新op_table.json中的 时间戳 ,用于真实生产环境。
```
调试好配置文件之后，将此脚本通过cron加入到定时任务中，即可实现每日定期增量备份的功能。

#### 3.4.3 数据导入预处理
在数据进入DolphinDB分布式库之前，某些场景需要对数据做一些预处理，比如数据格式转换，参照值转换等。这一功能可以通过自定义`saveFunctionDef`来实现，在数据上传到DolphinDB内存中，若`saveFunctionDef`有定义，插件会用此函数来替换tableInsert函数，将数据处理逻辑插入数据写入之前即可实现上述的功能。
此函数定义必须有三个参数： dbName, tbName, data，分别对应数据库名(不包含dfs://部分)，数据表名称，待写入的数据。


#### 附录:

* 更新分区表和维度表脚本模板代码(供参考)

    * savePartitionedData
    ```
    def rowUpdate(dbName, tbName, data, t){
    	updateRowCount = exec count(*) from ej(t,data,['OBJECT_ID'])
    	if(updateRowCount<=0) return
    	dfsPath = "dfs://" + dbName
    	temp = select * from t
    	cp = t.schema().chunkPath.substr(strlen("/" + dbName))
    	update temp set EVENT_ID = data.EVENT_ID,S_INFO_WINDCODE = data.S_INFO_WINDCODE, S_ACTIVITIESTYPE=data.S_ACTIVITIESTYPE, S_SURVEYDATE=data.S_SURVEYDATE, S_SURVEYTIME=data.S_SURVEYTIME, ANN_DT = data.ANN_DT,OPDATE=data.OPDATE,OPMODE=data.OPMODE where OBJECT_ID in data.OBJECT_ID
    	dropPartition(database(dfsPath), cp, tbName)
    	loadTable(dfsPath, tbName).append!(temp)
    }
    
    def savePartitionedData(dbName, tbName, data){
    	dfsPath = "dfs://" + dbName
    	login("admin","123456")
    	t = loadTable(dfsPath, tbName)
    	ds1 = sqlDS(<select * from t>)
    	mr(ds1, rowUpdate{dbName, tbName, data})
    }
    ```
    
    * saveDimensionData
    ```
    def saveDimensionData(dbName, tbName, data){
            login('admin','123456')
            dfsPath = 'dfs://' + dbName
            temp = select * from loadTable(dbPath, tbName)
            update temp set EVENT_ID = data.EVENT_ID,S_INFO_WINDCODE = data.S_INFO_WINDCODE, S_ACTIVITIESTYPE=data.S_ACTIVITIESTYPE, S_SURVEYDATE=data.S_SURVEYDATE, S_SURVEYTIME=data.S_SURVEYTIME, ANN_DT = data.ANN_DT,OPDATE=data.OPDATE,OPMODE=data.OPMODE where OBJECT_ID in data.OBJECT_ID
            db = database(dbPath)
            db.dropTable(tbName)
            dt = db.createTable(temp, tbName)
            dt.append!(temp)}
    ```
* 配置文件示例

BASECODE.json
```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                "reader": {
                    "name": "oraclereader",
                    "parameter": {
                        "username": "root",
                        "password": "password",
                        "column": [
                            "*"
                        ],
                        "connection": [
                            {
                                "table": [
                                    "BASECODE"
                                ],
                                "jdbcUrl": [
                                    "jdbc:oracle:thin:@127.0.0.1:1521:helowin"
                                ]
                            }
                        ],
                        "where":"OPDATE > to_date('2020-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"
                    }
                },
                "writer": {
                    "name": "dolphindbwriter",
                    "parameter": {
                        "userId": "user",
                        "pwd": "pass",
                        "host": "127.0.0.1",
                        "port": 8848,
                        "dbPath": "dfs://TESTDB",
                        "tableName": "BASECODE",
                        "saveFunctionName":"savePartitionedData",
                        "saveFunctionDef":"def() {...}",
                        "table": [
                            {
                                "type": "DT_DOUBLE",
                                "name": "S_INFO_MIN_PRICE_CHG_UNIT"
                            },
                            {
                                "type": "DT_DOUBLE",
                                "name": "S_INFO_LOT_SIZE"
                            },
                            {
                                "type": "DT_STRING",
                                "name": "S_INFO_ENAME"
                            },
                            {
                                "type": "DT_TIMESTAMP",
                                "name": "OPDATE"
                            },
                            {
                                "type": "DT_STRING",
                                "name": "OPMODE"
                            },
                            {
                                "type": "DT_STRING",
                                "name": "OBJECT_ID",
                                "isKeyField" :true
                            },
                            {
                                "type": "DT_STRING",
                                "name": "S_INFO_WINDCODE"
                            },
                            {
                                "type": "DT_STRING",
                                "name": "S_INFO_ASHARECODE"
                            },
                            {
                                "type": "DT_STRING",
                                "name": "S_INFO_COMPCODE"
                            },
                            {
                                "type": "DT_STRING",
                                "name": "S_INFO_SECURITIESTYPES"
                            },
                            {
                                "type": "DT_STRING",
                                "name": "S_INFO_SECTYPENAME"
                            }
                        ]
                    }
                }
            }
        ]
    }
}

```

* 配置文件参数说明

* **host**

	* 描述：Server Host 

 	* 必选：是 <br />

	* 默认值：无 <br />

* **port**

	* 描述：Server Port  <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **userId**

	* 描述：DolphinDB 用户名 <br />
            导入分布式库时，必须要有权限的用户才能操作，否则会返回
	* 必选：是 <br />

	* 默认值：无 <br />

* **pwd**

	* 描述：DolphinDB 用户密码

	* 必选：是 <br />

	* 默认值：无 <br />

* **dbName**

	* 描述：需要写入的目标分布式库名称，比如"MYDB",本参数值不包含数据库路径的"dfs://"部分。

	* 必选：是 <br />

	* 默认值：无 <br />

* **tableName**

	* 描述: 目标数据表名称

	* 必须: 是

	* 默认值: 无

* **table**

	* 描述：写入表的字段集合。内部结构为
	```
	{"name": "columnName", "type": "DT_STRING", "isKeyField":true}
	```
	* name ：字段名称
	* isKeyField：是否唯一键值，可以允许组合唯一键
	* type 枚举值以及对应DataX数据类型如下。DolphinDB的数据类型及精度，请参考 https://www.dolphindb.cn/cn/help/DataType.html
	
	DolphinDB类型 | 配置值 | DataX类型
	---|---|---
	 DOUBLE | DT_DOUBLE | DOUBLE
     FLOAT|DT_FLOAT|DOUBLE|
     BOOL|DT_BOOL|BOOLEAN
     DATE|DT_DATE|DATE
     DATETIME|DT_DATETIME| DATE
     TIME|DT_TIME|DATE
     TIMESTAMP|DT_TIMESTAMP| DATE
     NANOTIME|DT_NANOTIME| DATE
     NANOTIMETAMP|DT_NANOTIMETAMP| DATE
     INT|DT_INT|LONG
     LONG|DT_LONG|LONG
     UUID|DT_UUID|STRING
     SHORT|DT_SHORT|LONG
     STRING|DT_STRING|STRING
     SYMBOL|DT_SYMBOL|STRING
	
	* 必选：是 <br />
	* 默认值：无 <br />

* **saveFunctionName**
    * 描述：自定义数据处理函数，插件在接收到reader的数据后，会将数据提交到DolphinDB并听过tableInsert函数写入指定库表，如果有定义此参数，则会用本函数替换tableInsert函数。
    * 必选：否 <br />
	* 枚举值： savePartitionedData(更新分布式表)/saveDimensionData(更新维度表) <br />
	   当saveFunctionDef未定义或为空时, saveFunctionName可以取枚举值之一，对应用于更新分布式表和维度表的数据处理。

* **saveFunctionDef**
    * 描述：自定义函数体定义。
	* 必选：当saveFunctionName参数不为空且非两个枚举值之一时，此参数必填 <br />
	* 默认值：无 <br />

