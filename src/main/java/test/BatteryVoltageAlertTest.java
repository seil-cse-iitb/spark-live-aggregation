package test;

import handlers.ConfigHandler;
import handlers.LogHandler;
import handlers.SparkHandler;
import handlers.UtilsHandler;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.ArrayList;

public class BatteryVoltageAlertTest {
    public static void main(String[] args) {
        final SparkHandler sparkHandler = new SparkHandler();
        final String[] sensorId = {
                "temp_k_204",
                "temp_k_ch",
                "temp_k_fck",
                "temp_k_lecture_hall",
                "temp_k_lh",
                "temp_k_seil",
                "temp_k_sic205",
                "temp_k_sic301",
                "temp_k_sic305",
                "temp_k_sr",
                "temp_ssic205"
        };
        for (int i = 0; i < sensorId.length; i++) {
            final int finalI = i;
            Thread thread = new Thread(new Runnable() {
                public void run() {
//                    System.out.println("Monitoring: "+sensorId[finalI]);
                    monitor(sparkHandler, sensorId[finalI]);
                }
            });
            thread.start();
        }
    }

    public static void monitor(SparkHandler sparkHandler, final String sensorId) {
        Dataset<Row> stream = sparkHandler.sparkSession
                .readStream()
                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                .option("topic", UtilsHandler.getTopic(sensorId))
                .option("localStorage", "/tmp/spark-mqtt/")
                .option("QoS", 0)
                .load(ConfigHandler.MQTT_URL);
        LogHandler.logInfo("[Monitoring]=>[SensorId:" + sensorId + "][Topic:" + UtilsHandler.getTopic(sensorId) + "]");
        Dataset<Row> emptyTable = sparkHandler.sparkSession.createDataFrame(new ArrayList<Row>(), ConfigHandler.SCHEMA_DHT_7);
        ExpressionEncoder<Row> rowExpressionEncoder = emptyTable.exprEnc();
        Dataset<Row> streamDataset = stream.map(new MapFunction<Row, Row>() {
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

        StreamingQuery console = streamDataset.writeStream()
//                .trigger(Trigger.ProcessingTime(20000))
                .outputMode(OutputMode.Update())
                .foreach(new ForeachWriter<Row>() {

                    @Override
                    public boolean open(long l, long l1) {
                        return true;
                    }

                    //String targetID{Should be unique}{here sensor_id concatenated with id column,
                    //level{danger,warn,info,success},type{},title,description;
                    @Override
                    public void process(Row row) {
                        System.out.println(row.toString());
                        double batteryVoltage = row.getDouble(row.fieldIndex("battery_voltage"));
                        double temperature = row.getDouble(row.fieldIndex("temperature"));

                        String targetId = URLEncoder.encode(row.getString(row.fieldIndex("sensor_id")) + "_" + row.getDouble(row.fieldIndex("id")));
                        String level, type, title, description;
                        if (batteryVoltage <= 3.1) {
                            System.out.println("BatteryVoltage: " + row);
                            type = "BatteryVoltage";
                            level = "warn";
                            title = URLEncoder.encode("Sensor Battery Low");
                            description = URLEncoder.encode("[" + targetId + "]The sensor's battery is low. Need to be charged. Please replace the battery and charge it.");
                            sendGetRequest(ConfigHandler.BMS_PORTAL_ALERT_URL + "?" +
                                    "target_id=" + targetId + "&level=" + level + "&type=" + type + "&title=" + title + "&description=" + description);
                        }
                        if (temperature > 80 || temperature <= 7) {
                            System.out.println("FaultyTemperature: " + row);
                            type = "FaultyTemperature";
                            level = "warn";
                            title = URLEncoder.encode("Faulty Temperature Sensor");
                            description = URLEncoder.encode("[" + targetId + "]Temperature sensor is giving false value: [Temperature:" + temperature + "]");
                            sendGetRequest(ConfigHandler.BMS_PORTAL_ALERT_URL + "?" +
                                    "target_id=" + targetId + "&level=" + level + "&type=" + type + "&title=" + title + "&description=" + description);
                        } else if (temperature > 32) {
                            System.out.println("AnomalousTemperature: " + row);
                            type = "AnomalousTemperature";
                            level = "danger";
                            title = URLEncoder.encode("Anomalous Temperature Detected!");
                            description = URLEncoder.encode("[" + targetId + "]Unusual temperature readings found (might be fire): [Temperature:" + temperature + "]");
                            sendGetRequest(ConfigHandler.BMS_PORTAL_ALERT_URL + "?" +
                                    "target_id=" + targetId + "&level=" + level + "&type=" + type + "&title=" + title + "&description=" + description);
                        }

                    }

                    @Override
                    public void close(Throwable throwable) {

                    }

                    private void sendGetRequest(final String url) {
                        new Thread(new Runnable() {
                            public void run() {
                                try {
                                    UtilsHandler.makeGetRequest(url);
                                } catch (Exception e) {
                                    LogHandler.logError("[GET_REQUEST:" + url + "]" + e.getMessage());
                                }
                            }
                        }).start();
                    }

                })
                .start();
        try {
            console.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
