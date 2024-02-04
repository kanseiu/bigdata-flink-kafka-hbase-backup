package com.kanseiu.flink.handler;

import com.kanseiu.flink.config.KafkaConfig;
import com.kanseiu.flink.config.ZookeeperConfig;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class OrderDetailDataBackup {

    private static final String SOURCE_TABLE_NAME = "order_detail_source_table";
    private static final String SINK_TABLE_NAME = "order_detail_sink_table";
    private static final String HBASE_TABLE_NAME = "ods:order_detail";
    private static final String KAFKA_TOPIC = "fact_order_detail";
    private static final String CONSUMER_GROUP_ID = "order-detail-backup-group";

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 创建源表
        String sourceTableCreateSql =
                "CREATE TEMPORARY TABLE " + SOURCE_TABLE_NAME + " ( " +

                        // 表字段定义
                        "   order_detail_id INTEGER," +
                        "   order_sn STRING," +
                        "   product_id INTEGER," +
                        "   product_name STRING," +
                        "   product_cnt INTEGER," +
                        "   product_price DOUBLE," +
                        "   average_cost DOUBLE," +
                        "   weight DOUBLE," +
                        "   fee_money DOUBLE," +
                        "   w_id INTEGER," +
                        "   create_time STRING," +
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

                        "   order_detail_id INTEGER," +
                        "   order_sn STRING," +
                        "   product_id INTEGER," +
                        "   product_name STRING," +
                        "   product_cnt INTEGER," +
                        "   product_price DOUBLE," +
                        "   average_cost DOUBLE," +
                        "   weight DOUBLE," +
                        "   fee_money DOUBLE," +
                        "   w_id INTEGER," +
                        "   create_time STRING," +
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
                concat(randInteger(10).cast(DataTypes.STRING()),"+", dateFormat(currentTimestamp(),"yyyyMMddHHmmssSSS"))
                        .as("rowkey"),
                row(
                        $("order_detail_id"),
                        $("order_sn"),
                        $("product_id"),
                        $("product_name"),
                        $("product_cnt"),
                        $("product_price"),
                        $("average_cost"),
                        $("weight"),
                        $("fee_money"),
                        $("w_id"),
                        $("create_time"),
                        $("modified_time")
                ).as("Info")
        );
        processedSourceTable.executeInsert(SINK_TABLE_NAME);
    }
}