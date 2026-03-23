# Spark Extension 交接说明

更新时间: 2026-03-23

这份文档只聚焦我实际维护的内容:

- 分支: `gravitino_main` 相关工作流
- 目标: 最终产出 Spark 侧可用的 extension / integration jar

## 11. 实际测试与复现

这一节只写我在本地真实跑通过的联调方式，方便后续接手人直接复现。

### 11.1 测试目标

目标不是只验证 Spark 能启动，而是验证:

- Spark 使用 Gravitino catalog 访问表
- OpenLineage 事件能发到 Marquez
- dataset 命名能落成 Gravitino 风格

即:

- `namespace = <metalake>`
- `name = <catalog>.<schema>.<table>`

我本地验证通过的 dataset 有:

- `hive.default.olg_hive_src`
- `iceberg.default.olg_ice_src`
- `paimon.default.olg_paimon_src`
- `mysql3307.ol_mysql.olg_mysql_src`

它们在 Marquez 中都落在:

- `namespace = test`

### 11.2 前置服务

本地复现前至少要保证这些服务已启动:

- Gravitino server: `http://localhost:8090`
- Marquez: `http://127.0.0.1:3000`
- Hive Metastore: `thrift://localhost:9083`
- HDFS NameNode: `hdfs://127.0.0.1:9000`
- MySQL: `127.0.0.1:3307`

MySQL 账号信息是:

- user: `root`
- password: `abc123`

### 11.3 Spark SQL 启动命令 demo

我本地实际使用的是类似下面这条命令。

注意:

- `openlineage-spark_*.jar` 是你编出来的 plugin jar
- `gravitino-spark-connector-runtime-*.jar` 是 Spark connector runtime
- OpenLineage 这里是直接发到 Marquez

```bash
/path/to/spark-3.5.3-bin-hadoop3/bin/spark-sql -v \
--jars /${path}/openlineage-spark_2.12-${gravitino-specific-version}.jar,/${path}/gravitino-spark-connector-runtime-3.5_2.12-${version}.jar,/${path}/iceberg-spark-runtime-3.5_2.12-${iceberg-version}.jar,/${path}/paimon-spark-3.5-${paimon-version}.jar,/${path}/mysql-connector-java-8.0.15.jar \
--conf spark.plugins="org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin" \
--conf spark.driver.extraJavaOptions="--add-opens java.base/java.security=ALL-UNNAMED" \
--conf spark.sql.gravitino.uri=http://localhost:8090 \
--conf spark.sql.gravitino.metalake=test \
--conf spark.sql.gravitino.enableIcebergSupport=true \
--conf spark.sql.gravitino.enablePaimonSupport=true \
--conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
--conf spark.openlineage.transport.type=http \
--conf spark.openlineage.transport.url=http://127.0.0.1:3000 \
--conf spark.openlineage.transport.endpoint=/api/v1/lineage \
--conf spark.openlineage.namespace=test \
--conf spark.openlineage.appName=demo_for_gravitino2 \
--conf spark.openlineage.columnLineage.datasetLineageEnabled=true \
--conf spark.sql.warehouse.dir=hdfs://127.0.0.1:9000/user/hive/warehouse-hive \
--conf spark.hadoop.fs.AbstractFileSystem.gvfs.impl=org.apache.gravitino.filesystem.hadoop.Gvfs \
--conf spark.hadoop.fs.gvfs.impl=org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem \
--conf spark.hadoop.fs.gravitino.server.uri=http://localhost:8090 \
--conf spark.hadoop.fs.gravitino.client.metalake=test
```

我本机实际联调脚本在:

- `/Users/fanng/deploy/openlineage/spark-gravitino.sh`

### 11.4 SQL demo: Hive -> Iceberg -> Paimon

下面这组 SQL 是我本地反复复现时用的最小链路:

```sql
drop table if exists paimon.default.olg_paimon_src;
drop table if exists iceberg.default.olg_ice_src;
drop table if exists hive.default.olg_hive_src;

create table hive.default.olg_hive_src (id int, label string);

insert overwrite table hive.default.olg_hive_src values
  (1, 'hive_a'),
  (2, 'hive_b');

create table iceberg.default.olg_ice_src as
select * from hive.default.olg_hive_src;

create table paimon.default.olg_paimon_src as
select * from iceberg.default.olg_ice_src;

select * from hive.default.olg_hive_src;
select * from iceberg.default.olg_ice_src;
select * from paimon.default.olg_paimon_src;
```

预期 `select` 结果:

```text
1  hive_a
2  hive_b
```

### 11.5 SQL demo: MySQL JDBC catalog

我本地还额外建了一个 MySQL catalog:

- catalog: `mysql3307`
- schema: `ol_mysql`
- table: `olg_mysql_src`

Spark 侧最小验证 SQL:

```sql
show namespaces in mysql3307;
show tables in mysql3307.ol_mysql;
select * from mysql3307.ol_mysql.olg_mysql_src;
```

预期结果:

```text
ol_mysql
olg_mysql_src

1  mysql_a
2  mysql_b
```

### 11.6 MySQL catalog 创建 demo

这个 catalog 是我在 Gravitino server 中手动创建的，供 Spark 直接通过 Gravitino catalog 访问:

```bash
curl -X POST http://localhost:8090/api/metalakes/test/catalogs \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"mysql3307",
    "type":"relational",
    "provider":"jdbc-mysql",
    "comment":"mysql on 3307 for lineage test",
    "properties":{
      "jdbc-url":"jdbc:mysql://127.0.0.1:3307?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
      "jdbc-user":"root",
      "jdbc-password":"abc123",
      "jdbc-driver":"com.mysql.cj.jdbc.Driver"
    }
  }'
```

如果本地 MySQL 没有测试表，可以先建:

```bash
mysql -h 127.0.0.1 -P 3307 -u root -pabc123 -e "
create database if not exists ol_mysql;
create table if not exists ol_mysql.olg_mysql_src (id int, label varchar(32));
delete from ol_mysql.olg_mysql_src;
insert into ol_mysql.olg_mysql_src values (1,'mysql_a'),(2,'mysql_b');
"
```

### 11.7 联调时踩过的坑

这几个问题会直接导致你误判 extension / plugin 有问题:

1. `show catalogs;` 可能只显示 `spark_catalog`
   
   但这不代表 Gravitino catalog 没有挂进去。
   
   更可靠的检查方法是:

   ```sql
   show namespaces in hive;
   show namespaces in iceberg;
   show namespaces in paimon;
   show namespaces in mysql3307;
   ```

2. Hive warehouse 没写权限时，`create table hive.default.xxx` 会失败
   
   我本地最后是把下面两个目录放开了写权限:

   - `/user/hive/warehouse`
   - `/user/hive/warehouse-hive`

3. MySQL catalog 如果没带 `allowPublicKeyRetrieval=true`，经常会碰到:
   
   - `Public Key Retrieval is not allowed`

4. 如果 OpenLineage plugin 和 Spark connector 不在一致的 classloader 路径下，日志里可能会出现 reflective access warning
   
   但这不一定会阻止功能。

### 11.8 如何判断这次测试算通过

最核心不是看 Spark 日志里有没有报 warning，而是看 Marquez 中最终落下来的 dataset 标识。

这次测试通过的标准是:

- Hive 读写后能看到 `hive.default.olg_hive_src`
- Iceberg CTAS 后能看到 `iceberg.default.olg_ice_src`
- Paimon CTAS 后能看到 `paimon.default.olg_paimon_src`
- MySQL 读取后能看到 `mysql3307.ol_mysql.olg_mysql_src`

并且这些 dataset 都在:

- `namespace = test`

如果又回退成下面这类名字，基本就说明没真正走到 Gravitino catalog 语义:

- `spark_catalog.default.xxx`
- `file:/...`
