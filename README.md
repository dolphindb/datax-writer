# 基于 DataX 的 DolphinDB 数据导入工具

DataX-dolphindbwriter 是为解决用户将不同数据来源的数据同步到 DolphinDB 而开发的插件。这类数据的特征是改动很少，并且数据分散在不同的数据库系统中。

## 1. DataX 离线数据同步

DataX 是在阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、DRDS 等各种异构数据源之间高效的数据同步功能，详情可查看 [DataX 已支持的数据源](https://github.com/alibaba/DataX/blob/master/README.md#support-data-channels)。

DataX 是可扩展的数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的 Reader 插件，以及向目标端写入数据的 Writer 插件。理论上 DataX 框架可以支持任意数据源类型的数据同步工作。每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。

### DataX 插件：dolphindbwriter

基于 DataX 的扩展功能，dolphindbwriter 插件实现了向 DolphinDB 写入数据。使用 DataX 的现有 reader 插件结合 DolphinDBWriter 插件，即可满足从不同数据源向 DolphinDB 同步数据。

DolphinDBWriter 底层依赖 DolphinDB Java API，采用批量写入的方式将数据写入分布式数据库。

本插件通常用于以下两个场景：

1. 定期从数据源向 DolphinDB 追加新增数据。
2. 定期获取更新的数据，定位 DolphinDB 中的相同数据并进行更新。此种模式下，由于需要将历史数据读取出来并在内存中进行匹配，会需要大量的内存，因此这种场景适用于在 DolphinDB 中容量较小的表，通常建议使用数据量在 200 万以下的表。

当前使用的更新数据的模式是通过全表数据提取、更新后删除分区重写的方式来实现。**注意，目前版本还无法保障上述整体操作的原子性，后续版本会针对此种方式的事务处理方面进行优化和改进。**

## 2. 使用方法

详细信息请参阅 [DataX 指南](https://github.com/alibaba/DataX/blob/master/userGuid.md)，以下仅列出必要步骤。**注意，dataX 的启动脚本基于 python2 开发，所以需要使用 python2 来执行 datax.py。**

### 2.1 下载部署 DataX

[点击下载 DataX](https://datax-opensource.oss-cn-hangzhou.aliyuncs.com/202210/datax.tar.gz)。

### 2.2 部署 DataX-DolphinDBWriter 插件

将源码的 `./dist/dolphindbwriter` 目录下所有内容拷贝到 `datax/plugin/writer` 目录下，即可使用。

### 2.3 执行 DataX 任务

进入 `datax/bin` 目录下，用 python 执行 `datax.py` 脚本，并指定配置文件地址，示例如下：

```
cd /root/datax/bin/
python datax.py /root/datax/myconf/BASECODE.json
```

### 2.4 导入实例

使用 DataX 的绝大部分工作都是通过配置来完成，包括双边的数据库连接信息和需要同步的数据表结构信息等。

#### 2.4.1 全量导入

下面以从 oracle 向 DolphinDB 导入一张表 BASECODE 进行示例.

首先在导入前，需要在 DolphinDB 中预先创建好目标数据库和表；然后使用 oraclereader 从 oracle 读取 BASECODE 表读取全量数据；再使用 dolphindbwriter 将读取到的 BASECODE 数据写入 DolphinDB 中。

编写配置文件 BASECODE.json，并存放到指定目录，比如 `/root/datax/myconf` 目录下，配置文件说明参考本文附录。**注意：在做全量导入时，*saveFunctionName* 和 *saveFunctionDef* 这两项无需配置，删除即可。**

配置完成后，在 `datax/bin` 目录下执行如下脚本即可启动同步任务

```
cd /root/datax/bin/
python datax.py /root/datax/myconf/BASECODE.json
```

#### 2.4.2 增量数据导入

增量数据分两种类型，一种是新增数据，另一种是已有数据的更新，即更新了数据内容以及时间戳。对于这两种类型的数据要用不同的数据导入方式。

* **新增数据增量同步**
    新增数据的增量同步与全量导入相比，唯一的不同点在于 reader 对数据源中已导入数据的过滤。通常需要处理增量数据的表，都会有一个时间戳的列来标记入库时间，在 oralce reader 插件中，只需要配置 where 条件，增加时间戳过滤即可，其对于 dolphindbwriter 的配置与全量导入完全相同。比如时间戳字段为 OPDATE， 要增量导入 2020.03.01 之后的增量数据，则配置 `"where": "OPDATE > to_date('2020-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')`。
* **变更数据增量同步**
  * 变更数据在数据源有不同的记录方法，比较规范的方法是通过一个变更标志和时间戳来记录，比如用 OPTYPE、 OPDATE 来记录变更的类型和时间戳，这样可以通过类似 `"where": "OPTYPE=1 and OPDATE > to_date('2020-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')` 条件过滤出增量数据。
    对于 writer 的配置项，需要增加如下两处配置:
    * isKeyField
    因为变更数据的更新需要目标表中有唯一列，所以 writer 的配置中，需要对 table 配置项中唯一键列增加 `isKeyField=true` 这一配置项。
    * saveFunctionName
    DolphinDB 有多种数据存储的方式，比较常用的两种是分布式表和维度表。dolphindbwriter 中内置了更新这两种表的脚本模板，当从数据源中过滤出变更数据之后，在 writer 配置中增加 `saveFunctionName` 和 `saveFunctionDef` 两个配置(具体用法请参考附录)，writer 会根据这两个配置项，采用对应的方式将数据更新到 DolphinDB 中。  
    在 1.30.21.4 版本中，用户可通过 `saveFunctionName` 和 `saveFunctionDef` 引入 DolphinDB 的[upsert!](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/u/upsert%21.html)功能以保证导入后的数据唯一性。具体配置示例参考附录。
  * 当有些数据源中不包含 OPTYPE 这一标识列，无法分辨出新数据是更新或是新增的时候，可以作为新增数据入库，以函数视图输出的方式：
    * 数据作为新增数据处理。这种方式处理后，数据表中存在重复键值。
    * 定义 functionView 作为数据访问接口，在 functionView 中对有重复键值的数据仅输出时间戳最新的一条。
    * 用户不能直接访问表(可以取消非管理员用户访问表的权限)，统一通过 functionView 访问数据。
* **定时同步**
  * 利用 shell 脚本实现 DataX 定时增量数据同步
    DataX 从设计上用于离线数据的一次性同步场景，我们可以通过 shell 或 python 脚本等工程方式实现定时增量同步。由于 DataX 支持非常灵活的配置， 一种相对简单并且可靠的思路就是根据时间戳动态修改配置文件：
    1. 利用 DataX 的 reader 去目标数据库读取数据，并记录最新时间戳；
    2. 将这个最新时间戳写入到一个 json 文本文件(op_table.json)；
    3. 再次执行同步时用脚本来读取 json 文本文件， 并动态修改同步的配置文件；
    4. 执行修改后的配置文件(run_job.json)，进行增量同步。

    本脚本默认约定，每一个数据表中都有 OPDATE 和 OPMODE 两个字段，用于标记操作时间和操作方式。OPMODE=0 为新增数据，OPMODE=1，2为更新和删除数据。在运行时，根据保存的时间戳动态为 oraclereader 增加一个 where 配置项：
    ```
    OPMODE = 0 AND OPDATE > to_date('[最新时间戳]', 'YYYY-MM-DD HH24:MI:SS') //新增数据
    
    OPMODE > 0 AND OPDATE > to_date('[最新时间戳]', 'YYYY-MM-DD HH24:MI:SS') //更新数据
    ```

定时同步是通过 python 脚本实现，被包含在 datax 的发布包中。

增量同步脚本在根目录 `ddb_script` 下，下载后被保存在本地磁盘比如 `/root/ddb_script/`。假设 datax 根目录为 `/root/datax`， 配置文件放在 `/root/datax/myconf/` 目录下，则增量同步的使用方法为

```
cd /root/ddb_script/
python main.py /root/datax/bin/datax.py /root/datax/myconf/BASECODE.json [run_type]
```

`run_type` 参数为选项值，当前支持 [test|prod]，具体说明如下：

* 设置为 test 时，脚本实时打印 datax 输出的内容，此设置下不会向 op_table.json 文件更新时间戳，可供重复调试配置文件使用。
* 设置为 prod 时，执行后会更新 op_table.json 中的时间戳，用于真实生产环境。

调试好配置文件之后，将此脚本通过 cron 加入到定时任务中，即可实现每日定期增量备份的功能。

#### 2.4.3 数据导入预处理

在数据进入 DolphinDB 分布式库之前，某些场景下需要对数据做一些预处理，比如数据格式转换，参照值转换等。这一功能可以通过自定义 `saveFunctionDef` 来实现，在数据上传到 DolphinDB 内存中，若 `saveFunctionDef` 有定义，插件会用此函数来替换 tableInsert 函数，将数据处理逻辑插入数据写入之前即可实现上述的功能。
此函数定义必须有三个参数： *dbPath*, *tbName*, *data*，分别对应数据库路径(如 `dfs://db1`)，数据表名称，待写入的数据。

## 附录

* **更新分区表和维度表脚本模板代码（供参考）**

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
                        "batchSize": 1000000,
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
    * 描述：Server Host。
    * 必选：是。
    * 默认值：无。

  * **port**
    * 描述：Server Port。
    * 必选：是。
    * 默认值：无。

  * **username/userId**
    * 描述：DolphinDB 用户名。导入分布式库时，必须要有权限的用户才能操作，否则会返回。
    * 必选：是。
    * 默认值：无。

  * **password/pwd**
    * 描述：DolphinDB 用户密码。
    * 必选：是。
    * 默认值：无。

  * **dbPath**
    * 描述：需要写入的目标分布式库名称，比如 "dfs://MYDB"。
    * 必选：是。
    * 默认值：无。

  * **preSql**
  	* 描述：写入数据到目的表前，会先执行这里的 DolphinDB 脚本。
  	* 必选：否。
    * 默认值：无。

  * **postSql**
  	* 描述：写入数据到目的表后，会执行这里的 DolphinDB 脚本。
  	* 必选：否。
  	* 默认值：无。

  * **column**
  	* 描述：目的表需要写入数据的字段。注意：不能与 table  字段同时使用。
  	* 必选：否。
  	* 默认值：无。
  	* 使用说明：
  		* 字段之间用英文逗号分隔，例如: "column": ["id","name","age"]。
  		* 如果要依次写入全部列，使用表示，例如: "column": ["*"]。
  		* 不可以为空 “column": []，或者”column": [““]。
  		* 导入数据是按照原表的字段顺序，无法通过识别新表的字段名称而改变数据顺序。

  * **tableName**
    * 描述: 目标数据表名称。
    * 必选: 是。
    * 默认值: 无。

  * **batchSize**
    * 描述: datax 每次写入 dolphindb 的批次记录数。
    * 必须: 否。
    * 默认值: 10,000,000。

  * **table**
    * 描述：写入表的字段集合。内部结构为：`{"name": "columnName", "type": "DT_STRING", "isKeyField":true}`。请注意此处列定义的顺序，需要与原表提取的列顺序完全一致。
      * name：字段名称。
      * isKeyField：是否唯一键值，可以允许组合唯一键。本属性用于数据更新场景，用于确认更新数据的主键，若无更新数据的场景，无需设置。
      * type：枚举值以及对应 DataX 数据类型如下表。DolphinDB 的数据类型及精度请参考[数据类型](https://www.dolphindb.cn/cn/help/DataTypesandStructures/DataTypes/index.html)。

    |  DolphinDB 类型 |  配置值 |  DataX 类型|
	  |:-------------|:--------|:-----------|
  	|DOUBLE | DT_DOUBLE | DOUBLE|
    |FLOAT|DT_FLOAT|DOUBLE|
    |BOOL|DT_BOOL|BOOLEAN|
    |DATE|DT_DATE|DATE|
    |MONTH|DT_MONTH|DATE|
    |DATETIME|DT_DATETIME| DATE|
    |TIME|DT_TIME|DATE|
    |SECOND|DT_SECOND|DATE|
    |TIMESTAMP|DT_TIMESTAMP| DATE|
    |NANOTIME|DT_NANOTIME| DATE|
    |NANOTIMETAMP|DT_NANOTIMETAMP| DATE|
    |INT|DT_INT|LONG|
    |LONG|DT_LONG|LONG|
    |UUID|DT_UUID|STRING|
    |SHORT|DT_SHORT|LONG|
    |STRING|DT_STRING|STRING|
    |SYMBOL|DT_SYMBOL|STRING|

    * 必选：是。
    * 默认值：无。

* **saveFunctionName**
  * 描述：自定义数据处理函数。若未指定此配置，插件在接收到 reader 的数据后，会将数据提交到 DolphinDB 并通过 tableInsert 函数写入指定库表；如果定义此参数，则会用指定函数替换 tableInsert 函数。
  * 必选：否。
  * 默认值：无。 也可以指定自定义函数。
    插件内置了 savePartitionedData(更新分布式表)/saveDimensionData(更新维度表) 两个函数，当 saveFunctionDef 未定义或为空时，saveFunctionName 可以取枚举值之一，对应用于更新分布式表和维度表的数据处理。

* **saveFunctionDef**
  * 描述：数据入库自定义函数。此函数指 用dolphindb 脚本来实现的数据入库过程。此函数必须接受三个参数：*dfsPath*(分布式库路径)，*tbName*(数据表名)，*data*(从 datax 导入的数据，table 格式)
  * 必选：当 saveFunctionName 参数不为空且非两个枚举值之一时，此参数必填。
  * 默认值：无。

* 引入 DolphinDB 的 [`upsert!`](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/u/upsert%21.html) 功能，修改配置文件 BASECODE.json 中的 `writer` 部分。

    ``` 
    "saveFunctionName":"upsertTable",
    "saveFunctionDef":"ignoreNull=true;keyColNames=`id;sortColumns=`value"
    ```

# ReleaseNotes:

## 新功能

* 新增配置参数 *preSql*，支持在插入数据前预执行一段脚本。（**1.30.21.5**）
* 新增配置参数 *postSql*，支持在插入数据后预执行一段脚本。（**1.30.21.5**）
* 新增配置参数 *userName*，指定 DolphinDB 的用户名，与原有参数 *userId* 兼容。（**1.30.21.5**）
* 新增配置参数 *passWord*，指定 DolphinDB 用户名的对应密码，与原有参数 *pwd* 兼容。（**1.30.21.5**）
## 故障修复

* 故障修复：修复当连接出现异常时，未抛出正确报错信息的问题。（**1.30.21.5**）
