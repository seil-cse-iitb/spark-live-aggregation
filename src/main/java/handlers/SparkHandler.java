package handlers;

import handlers.ConfigHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkHandler implements scala.Serializable {
    public SparkSession sparkSession;

    public SparkHandler() {
        initSession();
        logsOff();
    }

    public void initSession() {
        SparkConf conf = new SparkConf().setIfMissing("spark.master","local[4]");

        sparkSession = SparkSession.builder().appName(ConfigHandler.SCRIPT_IDENTITY_TEXT)
                .config("spark.sql.warehouse.dir", "~/sparkHandler-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")
//                .master("spark://10.129.149.14:7077") //can't print on console..Don't know why
//                .master("local[*]")
                .config(conf)
                .getOrCreate();

    }


    public void logsOff() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }


    public Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", ConfigHandler.MYSQL_USERNAME);
        properties.setProperty("password", ConfigHandler.MYSQL_PASSWORD);
        return properties;
    }

    public Dataset<Row> getRowsByTableName(String tableName) {
        Properties properties = getProperties();
        Dataset<Row> rows = this.sparkSession.read().jdbc(ConfigHandler.MYSQL_URL, tableName, properties);
        return rows;
    }

}
