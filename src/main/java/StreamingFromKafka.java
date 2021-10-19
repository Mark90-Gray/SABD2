import entity.NYBusLog;
import metrics.Query1metrics;
import metrics.Query2metrics;
import metrics.Query3metrics;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import utils.NYBusLogSchema;

import java.util.Properties;

public class StreamingFromKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", "broker:29092");
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink");

        DataStream<NYBusLog> stream =
               env.addSource(new FlinkKafkaConsumer<>("flink", new NYBusLogSchema(), properties));
        Query1.run(stream);
        //Query2.run(stream);
        //Query3.run(stream);

        env.execute();
    }
}
