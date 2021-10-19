package metrics;

import entity.NYBusLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
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

public class Query3metrics {
    //private static final int WINDOW_SIZE = 24;      // giorno
    private static final int WINDOW_SIZE = 24 * 7;  // settimana
    private static final Double[] WEIGHTS ={0.5,0.3,0.2};

    public static void run(DataStream<NYBusLog> stream) throws Exception {
        DataStream<Tuple2<NYBusLog,Long>> timestampedAndWatermarked = stream
                .assignTimestampsAndWatermarks
                        (new BoundedOutOfOrdernessTimestampExtractor<NYBusLog>(Time.seconds(1)) {
                            @Override
                            public long extractTimestamp(NYBusLog logIntegerTuple2) {
                                return logIntegerTuple2.getDateOccuredOn();
                            }
                        }).filter(x -> x.getDelay() != -1)
                .filter(x-> x.getCompanyName().isEmpty()).
                        map(x-> new Tuple2<>(x,System.currentTimeMillis())).
                        returns(Types.TUPLE(Types.GENERIC(NYBusLog.class), Types.LONG));
        //timestampedAndWatermarked.print();

        // somma del delay per boro
        DataStream<String> result_q3m = timestampedAndWatermarked
                .keyBy(value->value.f0.getCompanyName())
                .timeWindow(Time.hours(WINDOW_SIZE))
                .aggregate(new ScoreAggregator(), new KeyBinder())
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ResultProcessAllWindows());

        //forse vuole il TextoOutputFormat
        result_q3m.writeAsText(String.format("output"+ "query3Metrics_%d.out",WINDOW_SIZE),
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    }

    public static class MyReasonCount {
        public String company;
        public Integer countMP=0; //mec problem
        public Integer countHT=0; // heavy traffic
        public Integer countOR=0; //other reason
        public Long ts=0L;

    }

    private static class ScoreAggregator implements AggregateFunction<Tuple2<NYBusLog,Long>, MyReasonCount, Tuple2<Double,Long>> {

        @Override
        public MyReasonCount createAccumulator() {
            return new MyReasonCount();
        }

        @Override
        public MyReasonCount add(Tuple2<NYBusLog,Long> myNy, MyReasonCount myReasonCount) {
            myReasonCount.company=myNy.f0.getCompanyName();
            if(myNy.f0.getDelay()>30){
                if(myNy.f0.getDelay_reason().equals("Heavy Traffic"))
                    myReasonCount.countHT+=2;
                else if(myNy.f0.getDelay_reason().equals("Mechanical Problem"))
                    myReasonCount.countMP+=2;
                else
                    myReasonCount.countOR+=2;
            }
            else {
                if(myNy.f0.getDelay_reason().equals("Heavy Traffic"))
                    myReasonCount.countHT++;
                else if(myNy.f0.getDelay_reason().equals("Mechanical Problem"))
                    myReasonCount.countMP++;
                else
                    myReasonCount.countOR++;
            }
            myReasonCount.ts= Math.max(myReasonCount.ts,myNy.f1);

            return myReasonCount;
        }

        @Override
        public Tuple2<Double,Long> getResult(MyReasonCount myReasonCount) {

            return  new Tuple2<>(myReasonCount.countMP*WEIGHTS[0]+myReasonCount.countHT*WEIGHTS[1]+myReasonCount.countOR*WEIGHTS[2],myReasonCount.ts);
        }

        @Override
        public MyReasonCount merge(MyReasonCount a, MyReasonCount b) {
            a.countOR+=b.countOR;
            a.countMP+=b.countMP;
            a.countHT+=b.countHT;
            if (b.ts>a.ts)
                a.ts=b.ts;
            return a;
        }
    }

    private static class KeyBinder
            extends ProcessWindowFunction<Tuple2<Double,Long>, Tuple2<String, Tuple2<Double,Long>>, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple2<Double,Long>> classified,
                            Collector<Tuple2<String, Tuple2<Double,Long>>> out) {
            Tuple2<Double,Long> score = classified.iterator().next();
            out.collect(new Tuple2<>(key, score));
        }
    }

    private static class ResultProcessAllWindows
            extends ProcessAllWindowFunction<Tuple2<String, Tuple2<Double,Long>>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Tuple2<String, Tuple2<Double,Long>>> iterable, Collector<String> collector) {
            List<Tuple2<String, Tuple2<Double,Long>>> classifiedList = new ArrayList<>();
            Tuple2<String,Long> max_tuple=null;
            boolean first=true;

            for (Tuple2<String, Tuple2<Double,Long>>t : iterable){
                classifiedList.add(t);
                if(first){
                    max_tuple= new Tuple2<>(t.f0,t.f1.f1);
                }
                else if(max_tuple.f1<t.f1.f1)
                    max_tuple=new Tuple2<>(t.f0,t.f1.f1);
            }
            classifiedList.sort((a, b) -> new Double(100*(b.f1.f0 - a.f1.f0)).intValue());

            //StringBuilder result = new StringBuilder(Long.toString(context.window().getStart() /1000));
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            StringBuilder result = new StringBuilder(startDate.toString()+";");
            Long currentTime=System.currentTimeMillis();
            //result.append(" Latency_Window: "+(currentTime - max_tuple.f1));
            result.append(currentTime - max_tuple.f1);

            collector.collect(result.toString());
        }

    }
}
