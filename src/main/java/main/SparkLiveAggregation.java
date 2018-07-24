package main;

import handlers.*;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Some;
import scala.concurrent.duration.Duration;

import static handlers.ConfigHandler.*;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;

public class SparkLiveAggregation implements Serializable {

    String sensorId;//temp_k_205
    String toTableName;
    double startTs;
    String timeField;
    SparkHandler sparkHandler;
    String tableNameForSchema;
    String[] SQL_AGGREGATION_FORMULA;
    StructType SCHEMA;
    MySQLHandler mySQLHandler = new MySQLHandler(MYSQL_HOST, MYSQL_USERNAME, MYSQL_PASSWORD, "seil_sensor_agg_data");

    public SparkLiveAggregation(String tableNameForSchema, String sensorId, String toTableName) {
        this.sensorId = sensorId;
        this.toTableName = toTableName;
        this.timeField = "ts";
        this.tableNameForSchema = tableNameForSchema;
        this.startTs = -1;
        this.sparkHandler = new SparkHandler();
        this.SCHEMA = getSchema(tableNameForSchema);
        this.SQL_AGGREGATION_FORMULA = getSQLAggregationFormula(tableNameForSchema);
    }

    public SparkLiveAggregation(SparkHandler sparkHandler, String tableNameForSchema, String sensorId, String toTableName) {
        this.sensorId = sensorId;
        this.toTableName = toTableName;
        this.timeField = "ts";
        this.tableNameForSchema = tableNameForSchema;
        this.startTs = -1;
        this.sparkHandler = sparkHandler;
        this.SCHEMA = getSchema(tableNameForSchema);
        this.SQL_AGGREGATION_FORMULA = getSQLAggregationFormula(tableNameForSchema);
    }

    private String[] getSQLAggregationFormula(String tableNameForSchema) {
        if (tableNameForSchema.equalsIgnoreCase("sch_3")) {
            return ConfigHandler.SQL_AGGREGATION_FORMULA_SCH_3;
        } else if (tableNameForSchema.equalsIgnoreCase("temp_5")) {
            return ConfigHandler.SQL_AGGREGATION_FORMULA_TEMP_5;
        }
        return null;
    }

    private StructType getSchema(String tableNameForSchema) {
        if (tableNameForSchema.equalsIgnoreCase("sch_3")) {
            return ConfigHandler.SCHEMA_SCH_3;
        } else if (tableNameForSchema.equalsIgnoreCase("temp_5")) {
            return ConfigHandler.SCHEMA_TEMP_5;
        }
        return null;
    }

    public Dataset<Row> startAggregationKafka() {
        String topic = UtilsHandler.getTopic(sensorId);
        LogHandler.logInfo("[" + sensorId + "]Topic(" + topic + ")]");

        // Subscribe to 1 topic
        Dataset<Row> stream = sparkHandler.sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.129.149.18:9092,10.129.149.19:9092,10.129.149.20:9092")
//                .option("startingOffsets", "earliest")
                .option("subscribe", "data")
                .load();
        Dataset<Row> key_value = stream.select(functions.col("key").cast("string"),
                functions.col("value").cast("string"));
        Dataset<Row> topic_key_value = key_value.select("key", "value")
                .where(functions.col("key").equalTo(topic));
        Dataset<Row> value = topic_key_value.select("value");
//        Dataset<Row> rowDataset = value.withColumn("temp", functions.split(functions.col("value"), ","));
        printOnConsole(key_value);

        return null;
    }

    public void startAggregation() {
        String topic = UtilsHandler.getTopic(sensorId);
        LogHandler.logInfo("[" + sensorId + "]Topic(" + topic + ")]");
        Dataset<Row> stream = sparkHandler.sparkSession
                .readStream()
                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                .option("topic", topic)
                .option("localStorage", "/tmp/spark-mqtt/")
                .option("QoS", 0)
                .load(ConfigHandler.MQTT_URL);
//        stream.printSchema();
        Dataset<Row> emptyTable = sparkHandler.sparkSession.createDataFrame(new ArrayList<Row>(), SCHEMA);
        ExpressionEncoder<Row> rowExpressionEncoder = emptyTable.exprEnc();
        Dataset<Row> dataset = stream.map(new MapFunction<Row, Row>() {
            public Row call(Row row) throws Exception {
                Timestamp timestamp = row.getTimestamp(row.fieldIndex("timestamp"));
                String value1 = row.getString(row.fieldIndex("value"));
                String[] split = value1.split(",");
                ArrayList<Object> rowValues = new ArrayList<Object>();
                rowValues.add(sensorId);
                rowValues.add((double) timestamp.getTime() / 1000);
                for (String s : split) {
                    rowValues.add(Double.parseDouble(s));
                }
                return RowFactory.create(rowValues.toArray());
            }
        }, rowExpressionEncoder);
//        dataset.printSchema();
        Dataset<Row> aggregatedData = aggregate(dataset);
//        aggregatedData.printSchema();
        printOnConsole(aggregatedData.withColumn("TS", aggregatedData.col("window.start").cast(DataTypes.DoubleType)).drop("window"));
    }

    private Dataset<Row> aggregate(Dataset<Row> dataset) {
        String aggStr = "";
        Column[] aggArray = new Column[SQL_AGGREGATION_FORMULA.length - 1];
        for (int i = 0; i < SQL_AGGREGATION_FORMULA.length - 1; i++)
            aggArray[i] = functions.expr(SQL_AGGREGATION_FORMULA[i + 1]);
        Column expr = functions.expr(SQL_AGGREGATION_FORMULA[0]);
        Column timestamp = functions.col(timeField).cast(DataTypes.TimestampType).as("eventTime");
        String[] columnStrs = dataset.columns();
        Column[] columns = new Column[columnStrs.length + 1];
        for (int i = 0; i < columnStrs.length; i++) {
            columns[i] = functions.col(columnStrs[i]);
        }
        columns[columns.length - 1] = timestamp;
        Dataset<Row> select = dataset.select(columns);
        return select.withWatermark("eventTime", "5 minutes")
                .groupBy(functions.window(timestamp, "1 minute"))
                .agg(expr, aggArray);
    }

    private void printOnConsole(Dataset<Row> dataset) {
//        dataset = dataset.repartition(1);
        StreamingQuery console = dataset.writeStream()
                .outputMode(OutputMode.Update())
                .trigger(Trigger.ProcessingTime(Duration.apply("2 min")))
                .foreach(new ForeachWriter<Row>() {
                    Connection mysqlConnection = null;

                    @Override
                    public boolean open(long l, long l1) {
//                        System.out.println("open=> partition:" + l + ",version:" + l1);
                        mysqlConnection = mySQLHandler.buildConnection();
                        return true;
                    }

                    @Override
                    public void process(Row row) {
                        String[] fieldNames = row.schema().fieldNames();
                        Object[] values = new Object[fieldNames.length];
                        for (int i = 0; i < fieldNames.length; i++) {
                            Object value = row.get(i);
                            if (value != null)
                                values[i] = "'" + value + "'";
                            else
                                values[i] = "NULL";
                        }
                        String replaceSQL = "replace into `" + toTableName + "` (`" + StringUtils.join(fieldNames, "`,`") + "`) "
                                + " values(" + StringUtils.join(values, ",") + ")";
                        try {
                            Statement statement = mysqlConnection.createStatement();
                            int result = statement.executeUpdate(replaceSQL);
                            LogHandler.logInfo("[" + sensorId + "][ReplaceQueryOutput: " + result + "]");
                        } catch (SQLException e) {
                            e.printStackTrace();
                            LogHandler.logError("[" + sensorId + "][SQLException]" + e.getMessage() + "\n[ReplaceSQL:" + replaceSQL + "]");
                        }
//                        System.out.println(row);
                    }

                    @Override
                    public void close(Throwable throwable) {
//                        System.out.println("close");
                        try {
                            mysqlConnection.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                            LogHandler.logError("[MySQLConnectionClosing]" + e.getMessage());
                        }
                    }
                })

//                 .format("console")
//                .option("truncate", false)

//                .option("checkpointLocation", "/tmp/checkpoint")
                //  .start("/tmp/result.csv");
                .start();
        try {
            console.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }

}
