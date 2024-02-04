package com.kanseiu.flink.handler;

import com.kanseiu.flink.config.KafkaConfig;
import com.kanseiu.flink.config.ZookeeperConfig;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.*;

public class OrderMasterDataBackup {

    private static final String SOURCE_TABLE_NAME = "order_master_source_table";
    private static final String SINK_TABLE_NAME = "order_master_sink_table";
    private static final String HBASE_TABLE_NAME = "ods:order_master";
    private static final String KAFKA_TOPIC = "fact_order_master";
    private static final String CONSUMER_GROUP_ID = "order-master-backup-group";

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 创建源表
        String sourceTableCreateSql =
                "CREATE TEMPORARY TABLE " + SOURCE_TABLE_NAME + " ( " +

                        // 表字段定义
                        "   order_id INTEGER," +
                        "   order_sn STRING," +
                        "   customer_id INTEGER," +
                        "   shipping_user STRING," +
                        "   province STRING," +
                        "   city STRING," +
                        "   address STRING," +
                        "   order_source INTEGER," +
                        "   payment_method INTEGER," +
                        "   order_money DOUBLE," +
                        "   district_money DOUBLE," +
                        "   shipping_money DOUBLE," +
                        "   payment_money DOUBLE," +
                        "   shipping_comp_name STRING," +
                        "   shipping_sn STRING," +
                        "   create_time STRING," +
                        "   shipping_time STRING," +
                        "   pay_time STRING," +
                        "   receive_time STRING," +
                        "   order_status STRING," +
                        "   order_point INTEGER," +
                        "   invoice_title STRING," +
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
                        "   Info ROW<order_id INTEGER," +
                        "   order_sn STRING," +
                        "   customer_id INTEGER," +
                        "   shipping_user STRING," +
                        "   province STRING," +
                        "   city STRING," +
                        "   address STRING," +
                        "   order_source INTEGER," +
                        "   payment_method INTEGER," +
                        "   order_money DOUBLE," +
                        "   district_money DOUBLE," +
                        "   shipping_money DOUBLE," +
                        "   payment_money DOUBLE," +
                        "   shipping_comp_name STRING," +
                        "   shipping_sn STRING," +
                        "   create_time STRING," +
                        "   shipping_time STRING," +
                        "   pay_time STRING," +
                        "   receive_time STRING," +
                        "   order_status STRING," +
                        "   order_point INTEGER," +
                        "   invoice_title STRING," +
                        "   modified_time STRING>," +
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
                        $("order_id"),
                        $("order_sn"),
                        $("customer_id"),
                        $("shipping_user"),
                        $("province"),
                        $("city"),
                        $("address"),
                        $("order_source"),
                        $("payment_method"),
                        $("order_money"),
                        $("district_money"),
                        $("shipping_money"),
                        $("payment_money"),
                        $("shipping_comp_name"),
                        $("shipping_sn"),
                        $("create_time"),
                        $("shipping_time"),
                        $("pay_time"),
                        $("receive_time"),
                        $("order_status"),
                        $("order_point"),
                        $("invoice_title"),
                        $("modified_time")
                ).as("Info")
        );
        processedSourceTable.executeInsert(SINK_TABLE_NAME);
    }
}