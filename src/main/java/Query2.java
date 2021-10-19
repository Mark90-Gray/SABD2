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

public class Query2 {

    //scegliere dimensione finsetra
    private static final int WINDOW_SIZE = 24;      // giorno
    //private static final int WINDOW_SIZE = 24 * 7;  // settimana

    public static void run(DataStream<NYBusLog> stream) throws Exception {
        DataStream<NYBusLog> timestampedAndWatermarked = stream
                .assignTimestampsAndWatermarks
                        (new BoundedOutOfOrdernessTimestampExtractor<NYBusLog>(Time.seconds(1)) {
                            @Override
                            public long extractTimestamp(NYBusLog nyTimeStamp) {
                                return nyTimeStamp.getDateOccuredOn();
                            }
                        }).filter(x -> !x.getTime_slot().equals("null"));

        DataStream<String> result_q2;
        result_q2 = timestampedAndWatermarked
                .keyBy(NYBusLog::getDelay_reason)
                .timeWindow(Time.hours(WINDOW_SIZE))
                .aggregate(new CountAggregator(), new KeyBinder())
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ResultProcessAllWindows());

        //Stampa sul File
        result_q2.writeAsText(String.format("out/output"+ "query2_%d.out",WINDOW_SIZE),
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    }
   //conteggia per ogni chiave il relativo delay nello slot AM e quello nello slot PM
    public static class MyReason {
        public String reason;
        public Integer countAM=0;
        public Integer countPM=0;
    }

    private static class CountAggregator implements AggregateFunction<NYBusLog, MyReason, Tuple2<Integer,Integer>> {

        @Override
        public MyReason createAccumulator() {
            return new MyReason();
        }

        @Override
        public MyReason add(NYBusLog myNy, MyReason myReason) {
            myReason.reason=myNy.getDelay_reason();
            if( myNy.getTime_slot().equals("AM"))
                myReason.countAM++;
            else if (myNy.getTime_slot().equals("PM"))
                myReason.countPM++;
            return myReason;
        }

        @Override
        public Tuple2<Integer,Integer> getResult(MyReason myReason) {

            return new Tuple2<>(myReason.countAM,myReason.countPM);
        }

        @Override
        public MyReason merge(MyReason a, MyReason b) {
            a.countPM+=b.countPM;
            a.countAM+=b.countAM;
            return a;
        }
    }
    // Assegna la chiave opportuna al risultato dell'aggregate
    private static class KeyBinder
            extends ProcessWindowFunction<Tuple2<Integer,Integer>, Tuple2<String, Tuple2<Integer,Integer>>, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple2<Integer,Integer>> counter,
                            Collector<Tuple2<String, Tuple2<Integer,Integer>>> out) {
            Tuple2<Integer,Integer> count = counter.iterator().next();
            out.collect(new Tuple2<>(key, count));
        }
    }

    //Ritorna il risultato ordinando tramite sort per le liste appartenenti agli slot AM e PM e di questi
    //ne prende solo i primi 3 per ogni lista
    private static class ResultProcessAllWindows
            extends ProcessAllWindowFunction<Tuple2<String, Tuple2<Integer,Integer>>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Tuple2<String, Tuple2<Integer,Integer>>> iterable,
                            Collector<String> collector) {
            List<Tuple2<String,Integer>> countListAM = new ArrayList<>();
            List<Tuple2<String,Integer>> countListPM = new ArrayList<>();
            for (Tuple2<String, Tuple2<Integer,Integer>> t : iterable){
                countListAM.add(new Tuple2<>(t.f0,t.f1.f0));
                countListPM.add(new Tuple2<>(t.f0,t.f1.f1));
            }
            countListAM.sort((a, b) -> b.f1 - a.f1);
            countListPM.sort((a, b) -> b.f1 - a.f1);

            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            StringBuilder result = new StringBuilder(String.valueOf(startDate));
            //StringBuilder result = new StringBuilder(Long.toString(context.window().getStart() /1000));

            int sizeAM = countListAM.size();
            int sizePM = countListPM.size();

            for (int i = 0; i < 3 && i < sizeAM; i++){
                if(i==0)
                    result.append(", "+" AM: ").append(countListAM.get(i).f0);
                else
                    result.append(", ").append(countListAM.get(i).f0);
            }

            for (int i = 0; i < 3 && i < sizePM; i++){
                if(i==0)
                    result.append("; PM: ").append(countListPM.get(i).f0);
                else
                    result.append(", ").append(countListPM.get(i).f0);
            }

            collector.collect(result.toString());
        }

    }
}
