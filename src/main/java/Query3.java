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

public class Query3 {
    //Scegliere dimensione finestra
    private static final int WINDOW_SIZE = 24;      // giorno
    //private static final int WINDOW_SIZE = 24 * 7;  // settimana

    //Definizione dei pesi per la funzione f(x)= #Mec Problem * 0,5 + #Heavy Traffic * 0,3 + #Other Reason * 0,2
    private static final Double[] WEIGHTS ={0.5,0.3,0.2};

    public static void run(DataStream<NYBusLog> stream) throws Exception {
        DataStream<NYBusLog> timestampedAndWatermarked = stream
                .assignTimestampsAndWatermarks
                        (new BoundedOutOfOrdernessTimestampExtractor<NYBusLog>(Time.seconds(1)) {
                            @Override
                            public long extractTimestamp(NYBusLog logIntegerTuple2) {
                                return logIntegerTuple2.getDateOccuredOn();
                            }
                        }).filter(x -> x.getDelay() != -1)
                .filter(x-> x.getCompanyName()!=null)
                .filter(x-> !x.getTime_slot().equals("null"));

        DataStream<String> result_q3 = timestampedAndWatermarked
                .keyBy(NYBusLog::getCompanyName)
                .timeWindow(Time.hours(WINDOW_SIZE))
                .aggregate(new ScoreAggregator(), new KeyBinder())
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ResultProcessAllWindows());

        //Stampa sul File
        result_q3.writeAsText(String.format("out/output"+ "query3_%d.out",WINDOW_SIZE),
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    }
    //Classe che conteggia per ogni compagnia i ritardi dovuti a MP, HT, OR
    public static class MyReasonCount {
        public String company;
        public Integer countMP=0; //mec problem
        public Integer countHT=0; // heavy traffic
        public Integer countOR=0; //other reason

    }

    private static class ScoreAggregator implements AggregateFunction<NYBusLog, MyReasonCount, Double> {

        @Override
        public MyReasonCount createAccumulator() {
            return new MyReasonCount();
        }

        // Se ritardo >30 minuti +2 al count relativo al ritardo
        @Override
        public MyReasonCount add(NYBusLog myNy, MyReasonCount myReasonCount) {
            myReasonCount.company=myNy.getCompanyName();
            if(myNy.getDelay()>30){
                if(myNy.getDelay_reason().equals("Heavy Traffic"))
                    myReasonCount.countHT+=2;
                else if(myNy.getDelay_reason().equals("Mechanical Problem"))
                    myReasonCount.countMP+=2;
                else
                    myReasonCount.countOR+=2;
            }
            else {
                if(myNy.getDelay_reason().equals("Heavy Traffic"))
                    myReasonCount.countHT++;
                else if(myNy.getDelay_reason().equals("Mechanical Problem"))
                    myReasonCount.countMP++;
                else
                    myReasonCount.countOR++;
            }
            return myReasonCount;
        }

        //f(x)= # delay MP * 0,5 + # delay HT * 0,3 + # delay OR * 0,2
        @Override
        public Double getResult(MyReasonCount myReasonCount) {

            return   myReasonCount.countMP*WEIGHTS[0]+myReasonCount.countHT*WEIGHTS[1]+myReasonCount.countOR*WEIGHTS[2];
        }

        @Override
        public MyReasonCount merge(MyReasonCount a, MyReasonCount b) {
            a.countOR+=b.countOR;
            a.countMP+=b.countMP;
            a.countHT+=b.countHT;
            return a;
        }
    }
    //Assegna chiave opportuna al risultato dell'aggregate
    private static class KeyBinder
            extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Double> classified,
                            Collector<Tuple2<String, Double>> out) {
            Double score = classified.iterator().next();
            out.collect(new Tuple2<>(key, score));
        }
    }

    //Definisce la classifica delle prime 5 compagnie con f(x) più alta tramite sort per ogni finestra
    private static class ResultProcessAllWindows
            extends ProcessAllWindowFunction<Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Tuple2<String, Double>> iterable, Collector<String> collector) {
            List<Tuple2<String, Double>> classifiedList = new ArrayList<>();
            for (Tuple2<String, Double> t : iterable)
                classifiedList.add(t);
            classifiedList.sort((a, b) -> new Double(100*(b.f1 - a.f1)).intValue());
            //StringBuilder result = new StringBuilder(Long.toString(context.window().getStart() /1000));
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
              context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            StringBuilder result = new StringBuilder(String.valueOf(startDate));

            int size = classifiedList.size();
            for (int i = 0; i< size && i<5; i++)
                result.append(", ").append(classifiedList.get(i).f0).append(", ").append(classifiedList.get(i).f1);

            collector.collect(result.toString());
        }

    }
}
