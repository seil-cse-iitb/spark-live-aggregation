package test;

import handlers.ConfigHandler;
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
                    System.out.println("Monitoring: "+sensorId[finalI]);
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
                .outputMode(OutputMode.Update())
                .foreach(new ForeachWriter<Row>() {
                    @Override
                    public boolean open(long l, long l1) {
                        return true;
                    }

                    @Override
                    public void process(Row row) {
                        double batteryVoltage = row.getDouble(row.fieldIndex("battery_voltage"));
                        double temperature = row.getDouble(row.fieldIndex("temperature"));
                        if (batteryVoltage <= 3.4) {
                            System.out.println("BatteryVoltage: "+row);
                        }
                        if(temperature>50){
                            System.out.println("Temperature: "+row);
                        }
                    }

                    @Override
                    public void close(Throwable throwable) {

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
