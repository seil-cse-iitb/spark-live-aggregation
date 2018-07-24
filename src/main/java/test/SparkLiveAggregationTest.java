package test;

import handlers.ConfigHandler;
import handlers.LogHandler;
import handlers.SparkHandler;
import main.SparkLiveAggregation;
import org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkLiveAggregationTest {
    public static void main(String[] args) {
        final SparkHandler sparkHandler = new SparkHandler();
        final String[] sensorId = {
                "power_k_a",
                "power_k_ch_a",
                "power_k_ch_l",
                "power_k_ch_p1",
                "power_k_ch_p2",
                "power_k_clsrm_ac1",
                "power_k_clsrm_ac2",
                "power_k_clsrm_ac3",
                "power_k_cr_a",
                "power_k_cr_p",
                "power_k_dil_a",
                "power_k_dil_l",
                "power_k_dil_p",
                "power_k_erts_a",
                "power_k_erts_l",
                "power_k_erts_p",
                "power_k_f2_a",
                "power_k_f2_l",
                "power_k_f2_p",
                "power_k_fck_a",
                "power_k_fck_l",
                "power_k_fck_p",
                "power_k_lab_od1",
                "power_k_lab_od2",
                "power_k_lab_od3",
                "power_k_m",
                "power_k_off_a",
                "power_k_off_l",
                "power_k_p",
                "power_k_seil_a",
                "power_k_seil_l",
                "power_k_seil_p",
                "power_k_sr_a",
                "power_k_sr_p",
                "power_k_wc_a",
                "power_k_wc_l",
                "power_k_wc_p",
                "power_k_yc_a",
                "power_k_yc_p",
                "power_lcc_202_l",
                "power_lcc_202_p",
                "power_lcc_23_a",
                "power_lcc_302_l",
                "power_lcc_302_p"
        };

        for (int i = 0; i < sensorId.length; i++) {
            final int finalI = i;
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    String tableNameForSchema = "sch_3";
                    String toTableName = "live_agg_sch_3";
                    try {
                        LogHandler.logInfo("[Thread][Start] started for sensor_id:" + sensorId[finalI]);
                        SparkLiveAggregation sparkLiveAggregation= new SparkLiveAggregation(sparkHandler,tableNameForSchema, sensorId[finalI], toTableName);
                        sparkLiveAggregation.startAggregation();
                        LogHandler.logInfo("[Thread][End] ended for sensor_id:" + sensorId[finalI]);
                    } catch (Exception e) {
                        e.printStackTrace();
                        LogHandler.logError("From table: sensor_id:[" + sensorId[finalI] + "] To table:[" + toTableName + "]" + e.getMessage());
                    }
                }
            });
            thread.start();
        }


//    SparkLiveAggregation sla = new SparkLiveAggregation(sparkHandler,"sch_3","power_k_erts_a","live_agg_sch_3");
//        sla.startAggregation();
//        SparkHandler spark  = new SparkHandler();
//        Dataset<Row> stream = spark.sparkSession
//                .readStream()
//                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
//                .option("topic", "data/kresit/dht/ch")
//                .option("localStorage", "/tmp/spark-mqtt/")
//                .option("clientId", "spmq")
//                .option("QoS",0)
//                .load(ConfigHandler.MQTT_URL);
//        stream.printSchema();
//        StreamingQuery console = stream.select("topic").writeStream().outputMode(OutputMode.Append()).format("console")
//                .option("truncate",false).start();
//        try {
//            console.awaitTermination();
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }

    }
}
