import entity.NYBusLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class Query1 {

    //impostare la scelta della dimensione della finestra
    private static final int WINDOW_SIZE = 24;      // giorno
    //private static final int WINDOW_SIZE = 24 * 7;  // settimana
    //private static final int WINDOW_SIZE = 24*30;  // mese

    public static void run(DataStream<NYBusLog> stream) {
        DataStream<NYBusLog> timestampedAndWatermarked = stream
                .assignTimestampsAndWatermarks
                        (new BoundedOutOfOrdernessTimestampExtractor<NYBusLog>(Time.seconds(1)) {
                            @Override
                            public long extractTimestamp(NYBusLog logIntegerTuple2) {
                                return logIntegerTuple2.getDateOccuredOn();
                            }
                        }).filter(x -> x.getDelay() != -1)
                .filter(x->!x.getBoro().isEmpty())
                .filter(x-> !x.getTime_slot().equals("null"));

        // somma delay per boro
        DataStream<String> result_q1 = timestampedAndWatermarked
                .keyBy(NYBusLog::getBoro).timeWindow(Time.hours(WINDOW_SIZE))
                .aggregate(new AvgAggregator(), new KeyBinder())
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ResultProcessAllWindows());

        result_q1.writeAsText(String.format("out/output"+ "query1_%d.out",WINDOW_SIZE),
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }
    //classe statica di appoggio
    public static class MyAverage {
        public String boro;
        public Integer count=0;
        public Double sum=0.0;
    }

    private static class AvgAggregator implements AggregateFunction<NYBusLog, MyAverage, Double> {

        @Override
        public MyAverage createAccumulator() {
            return new MyAverage();
        }

        @Override
        public MyAverage add(NYBusLog myNy, MyAverage myAverage) {
            myAverage.boro=myNy.getBoro();
            myAverage.count++;
            myAverage.sum=myAverage.sum+myNy.getDelay();
            return myAverage;
        }

        @Override
        public Double getResult(MyAverage myAverage) {

            return   myAverage.sum/myAverage.count;
        }

        @Override
        public MyAverage merge(MyAverage a, MyAverage b) {
            a.sum+=b.sum;
            a.count+=b.count;
            return a;
        }
    }

    //Assegna la chiave al risultato dell'aggregate
    private static class KeyBinder
            extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {
        @Override
        public void process(String key,
                            Context context,
                            Iterable<Double> average,
                            Collector<Tuple2<String, Double>> out) {
            Double avg = average.iterator().next();
            out.collect(new Tuple2<>(key, avg));
        }
    }

    //stampa i risultati con data di riferimento di inizio finestra
    private static class ResultProcessAllWindows
            extends ProcessAllWindowFunction<Tuple2<String, Double>, String, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Tuple2<String, Double>> iterable, Collector<String> collector) {
            List<Tuple2<String, Double>> averageList = new ArrayList<>();
            for (Tuple2<String, Double> t : iterable)
                averageList.add(t);
            //Debug per oridnare stampa
            /*averageList.sort((a, b) -> new Double(b.f1 - a.f1).intValue());*/
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            //StringBuilder result = new StringBuilder(Long.toString(context.window().getStart() /1000));
            StringBuilder result = new StringBuilder(String.valueOf(startDate));

            for (Tuple2<String, Double> stringDoubleTuple2 : averageList)
                result.append(", ").append(stringDoubleTuple2.f0).append(", ").append(stringDoubleTuple2.f1);

            collector.collect(result.toString());
        }

    }
}
