# Release Notes — DataX dolphindbwriter

## 1.30.22.5

本版本主题为**数据保真与类型支持增强**，共包含 5 个工单修复。

### 新增功能与修复

- **JAVAOS-1468｜新增 7 种数据类型支持，修复时间类型精度丢失**
  - 新增支持：MINUTE、DATEHOUR、IPADDR、INT128、COMPLEX、POINT；MONTH 补全字符串解析（`yyyy.MM'M'`）。
  - 时间类列（DATE/TIME/SECOND/DATETIME/TIMESTAMP/NANOTIME/NANOTIMESTAMP/MINUTE/DATEHOUR）在输入为字符串时按 DolphinDB 原生格式解析，恢复毫秒/纳秒精度——此前 TIME 的毫秒、NANOTIME/NANOTIMESTAMP 的纳秒会被丢弃。
  - 不支持的列类型改为在 task init 阶段即报错（`Column 'x' type DT_Y is not supported by dolphindbwriter.`），不再拖延到写入时以 NPE 或列错位形式失败。

- **JAVAOS-2069｜STRING/SYMBOL 中 NUL(`\u0000`) 处理改为删除而非截断**
  - 原行为：底层 javaapi 将字段从首个 NUL 起整段截尾（`abc\0def`→`abc`），且每个命中单元格打一条 WARN，大量数据时形成日志洪泛。
  - 新行为：写入前删除全部 NUL、保留其余内容（`abc\0def`→`abcdef`）；按列计数，`Task.post()` 汇总一条 WARN（`affected cells per column`），javaapi 的洪泛 WARN 不再出现。

- **JAVAOS-448｜整型列接受小数输入（四舍五入 + 范围检查）**
  - BYTE/SHORT/INT/LONG 列现在接受 `1.12` 类小数输入：先 `HALF_UP` 舍入到整数，再检查目标类型范围。
  - 越界值（含会被当作 NULL 哨兵的 `MIN_VALUE`）抛出 `ArithmeticException` 终止 task，不再静默写入 NULL 或报错 `NumberFormatException`。
  - 被舍入的单元格按列计数，`Task.post()` 汇总日志。

- **JAVAOS-2075｜修复 BLOB 列二进制数据静默损坏**
  - BLOB 写入改为原始字节通道（`byte[]`），此前经 UTF-8 字符串中转会把非 UTF-8 字节静默替换为 `U+FFFD`，图片/压缩包等二进制内容写坏且无告警。
  - 超长截断（>4MB）由"前 1398101 个字符"改为"前 4194304 个字节"。

- **JAVAOS-1466｜修复 CHAR 类型列写入**
  - CHAR（`DT_BYTE`）列支持 reader 产出的数值型字节值（`LongColumn`），此前对该类型调用 `asBytes()` 必抛转换异常。

### 行为变化（升级请注意）

- **小数写整型列**：由截断改为 `HALF_UP` 舍入（`1.9→2`、`-1.5→-2`），与服务端 `int()` 语义一致；
- **整型列边界值**：输入恰为类型最小值（`MIN_VALUE`，DolphinDB NULL 哨兵）现在报错而非写成 NULL；
- **BLOB 超 4MB 截断**：从字符截断改为字节截断，多字节文本可能在字符中间截断；
- **失败提前**：字段配置/表结构异常在 init 阶段即报错，错误信息更准确（原路径最终也会失败，只是报错晚且误导）。

### 兼容性

- 作业配置格式无变化；
- BLOB 列中合法 UTF-8 文本内容新旧路径写入结果逐字节一致；reader 产出 `StringColumn` 的文本上游行为不变；
- reader `table`/`column` 按位置映射语义不变。

### 已知限制

- 仍不支持的类型：DATEMINUTE、DURATION（dolphindbreader 侧亦存在缺陷，另行跟踪）；
- OLAP 引擎不支持 BLOB 列（服务端限制）；
- BLOB 二进制保真要求上游产出 `BytesColumn`（如 oraclereader 的 RAW/BLOB/VARBINARY 列）；dolphindbreader 读 BLOB 仍经 `getString()` 产出 StringColumn，二进制内容在上游即已损坏；
- 时间类型的纳秒精度仅在上游以字符串传递时恢复；上游 `DateColumn`（`java.util.Date`）本身只保留毫秒。

### 构建与部署

- 源码：`dolphindbwriter/src/main/java/com/alibaba/datax/plugin/writer/dolphindbwriter/`；
- 编译需 `--release 8`（JDK 8 字节码目标，避免 `Record` 类名歧义）；
- 部署包：`dist/dolphindbwriter/`，整目录拷贝至 `datax/plugin/writer/` 下使用。
