package com.kanseiu.flink.handler;

import com.kanseiu.flink.config.KafkaConfig;
import com.kanseiu.flink.config.ZookeeperConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 注意，这里的表字段数据类型做了一些调整，log_id 和 modified_time 都改为了 String;
 * 不明白为什么 log_id 的数据类型会被定义为 int，modified_time 被定义为 double
 */
public class ProductBrowseDataBackup {

    private static final String SOURCE_TABLE_NAME = "product_browse_source_table";
    private static final String SINK_TABLE_NAME = "product_browse_sink_table";
    private static final String HBASE_TABLE_NAME = "ods:product_browse";
    private static final String KAFKA_TOPIC = "log_product_browse";
    private static final String CONSUMER_GROUP_ID = "product-browse-backup-group";

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 创建源表
        String sourceTableCreateSql =
                "CREATE TEMPORARY TABLE " + SOURCE_TABLE_NAME + " (" +

                        // 表字段定义
                        "   log_id STRING," +
                        "   order_sn STRING," +
                        "   product_id INTEGER," +
                        "   customer_id STRING," +
                        "   gen_order STRING," +
                        "   modified_time STRING" +

                        // 连接kafka参数
                        ") WITH ('connector'='kafka'," +
                        "        'topic'='" + KAFKA_TOPIC + "'," +
                        "        'properties.bootstrap.servers'='" + KafkaConfig.KAFKA_BOOTSTRAP_SERVERS + "'," +
                        "        'properties.group.id'='" + CONSUMER_GROUP_ID + "'," +
                        "        'format'='json'," +
                        "        'scan.startup.mode'='latest-offset'" +
                        ")";

        tableEnvironment.executeSql(sourceTableCreateSql);

        // 创建Sink表
        String sinkTableCreateSql =
                "CREATE TEMPORARY TABLE " + SINK_TABLE_NAME + " (" +

                        // 表字段定义
                        "   rowkey STRING," +
                        "   Info ROW<" +

                        "   log_id STRING," +
                        "   order_sn STRING," +
                        "   product_id INTEGER," +
                        "   customer_id STRING," +
                        "   gen_order STRING," +
                        "   modified_time STRING" +

                        "   >," +
                        "   PRIMARY KEY (rowkey) NOT ENFORCED" +

                        // 连接hbase参数
                        ") WITH ('connector' = 'hbase-2.2'," +
                        "        'table-name' = '" + HBASE_TABLE_NAME + "'," +
                        "        'zookeeper.quorum' = '" + ZookeeperConfig.ZK_QUORUM + "'" +
                        " )";
        tableEnvironment.executeSql(sinkTableCreateSql);

        // 数据备份
        Table sourceTable = tableEnvironment.from(SOURCE_TABLE_NAME);
        Table processedSourceTable = sourceTable.select(
                concat($("log_id").substring(1, 1), "+", dateFormat(currentTimestamp(),"yyyy"), "+", $("log_id").substring(3))
                        .as("rowkey"),
                row(
                        $("log_id"),
                        $("order_sn"),
                        $("product_id"),
                        $("customer_id"),
                        $("gen_order"),
                        $("modified_time")
                ).as("Info")
        );
        processedSourceTable.executeInsert(SINK_TABLE_NAME);
    }
}