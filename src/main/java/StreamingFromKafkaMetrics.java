import entity.NYBusLog;
import metrics.Query1metrics;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import utils.NYBusLogSchema;

import java.util.Properties;

// classe utilizzata solo nella UI di Flink
public class StreamingFromKafkaMetrics {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "broker:29092");
        //properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink");

        DataStream<NYBusLog> stream =
                env.addSource(new FlinkKafkaConsumer<>("flink", new NYBusLogSchema(), properties));
        Integer size = Integer.parseInt(args[1]);
        Integer query = Integer.parseInt(args[0]);
        // size: giorno 24, settimana 24*7=168, mese 30*7=210
        // Query1: giorno, settimana, mese; Query2-Query3: giorno, settimana
        /*switch (query) {
            case 1:
                Query1.run(stream, size);
                break;
            case 2:
                Query2.run(stream, size);
                break;
            case 3:
                Query3.run(stream, size);
                break;
            default:
                Query1.run(stream, size);


        }
        env.execute();

 */

    }
}
