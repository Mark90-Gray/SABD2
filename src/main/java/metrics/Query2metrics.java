package metrics;

import entity.NYBusLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
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

public class Query2metrics {
    //private static final int WINDOW_SIZE = 24;      // giorno
    private static final int WINDOW_SIZE = 24 * 7;  // settimana

    public static void run(DataStream<NYBusLog> stream) throws Exception {
        DataStream<Tuple2<NYBusLog,Long>> timestampedAndWatermarked = stream
                .assignTimestampsAndWatermarks
                        (new BoundedOutOfOrdernessTimestampExtractor<NYBusLog>(Time.seconds(1)) {
                            @Override
                            public long extractTimestamp(NYBusLog nyTimeStamp) {
                                return nyTimeStamp.getDateOccuredOn();
                            }
                        }).filter(x -> !x.getTime_slot().equals("null"))
                .map(x-> new Tuple2<>(x,System.currentTimeMillis())).
                        returns(Types.TUPLE(Types.GENERIC(NYBusLog.class), Types.LONG));

        // somma del delay per boro
       DataStream<String> result_q2m = timestampedAndWatermarked
                .keyBy(value->value.f0.getDelay_reason())
                .timeWindow(Time.hours(WINDOW_SIZE))
                .aggregate(new CountAggregator(), new KeyBinder())
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ResultProcessAllWindows());

        result_q2m.writeAsText(String.format("output"+ "query2Metrics_%d.out",WINDOW_SIZE),
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    }

    public static class MyReason {
        public String reason;
        public Integer countAM=0;
        public Integer countPM=0;
        public Long ts=0L;
    }

    private static class CountAggregator implements AggregateFunction<Tuple2<NYBusLog,Long>, MyReason, Tuple3<Integer,Integer,Long>> {

        @Override
        public MyReason createAccumulator() {
            return new MyReason();
        }

        @Override
        public MyReason add(Tuple2<NYBusLog,Long> myNy, MyReason myReason) {
            myReason.reason=myNy.f0.getDelay_reason();
            if( myNy.f0.getTime_slot().equals("AM"))
                myReason.countAM++;
            else if (myNy.f0.getTime_slot().equals("PM"))
                myReason.countPM++;
            myReason.ts= Math.max(myReason.ts,myNy.f1);

            return myReason;
        }

        @Override
        public Tuple3<Integer,Integer,Long> getResult(MyReason myReason) {

            return new Tuple3<>(myReason.countAM,myReason.countPM,myReason.ts);
        }

        @Override
        public MyReason merge(MyReason a, MyReason b) {
            a.countPM+=b.countPM;
            a.countAM+=b.countAM;
            if (b.ts>a.ts)
                a.ts=b.ts;
            return a;
        }
    }

    private static class KeyBinder
            extends ProcessWindowFunction<Tuple3<Integer,Integer,Long>, Tuple2<String, Tuple3<Integer,Integer,Long>>, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple3<Integer,Integer,Long>> counter,
                            Collector<Tuple2<String, Tuple3<Integer,Integer,Long>>> out) {
            Tuple3<Integer,Integer,Long> count = counter.iterator().next();
            out.collect(new Tuple2<>(key, count));
        }
    }

    private static class ResultProcessAllWindows
            extends ProcessAllWindowFunction<Tuple2<String, Tuple3<Integer,Integer,Long>>, String, TimeWindow> {

        private transient Meter meter;

        @Override
        public void open(Configuration parameters) throws Exception {
            com.codahale.metrics.Meter doc= new com.codahale.metrics.Meter();
            this.meter = getRuntimeContext()
                    .getMetricGroup().addGroup("Query2").meter("Througput", new DropwizardMeterWrapper(doc));
        }
        @Override
        public void process(Context context, Iterable<Tuple2<String, Tuple3<Integer,Integer,Long>>> iterable,
                            Collector<String> collector) {
            List<Tuple2<String,Integer>> countListAM = new ArrayList<>();
            List<Tuple2<String,Integer>> countListPM = new ArrayList<>();
            boolean first=true;
            Tuple2<String,Long> max_tuple=null;

            for (Tuple2<String, Tuple3<Integer,Integer,Long>> t : iterable){
                countListAM.add(new Tuple2<>(t.f0,t.f1.f0));
                countListPM.add(new Tuple2<>(t.f0,t.f1.f1));
                if(first){
                    max_tuple= new Tuple2<>(t.f0,t.f1.f2);
                }
                else if(max_tuple.f1<t.f1.f2)
                    max_tuple=new Tuple2<>(t.f0,t.f1.f2);
            }
            countListAM.sort((a, b) -> new Integer(b.f1 - a.f1).intValue());
            countListPM.sort((a, b) -> new Integer(b.f1 - a.f1).intValue());

            Long currentTime=System.currentTimeMillis();
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);
            //StringBuilder result = new StringBuilder(Long.toString(context.window().getStart() /1000));
            StringBuilder result = new StringBuilder(startDate.toString()+";");

            String res2= " ";
            int sizeAM = countListAM.size();
            int sizePM = countListPM.size();
            this.meter.markEvent();
            //res2 = ","+ meter.getRate()+", ";
            //result.append(res2);
            result.append((currentTime - max_tuple.f1));
            for (int i = 0; i < 3 && i < sizeAM; i++){
                this.meter.markEvent();
                res2 = " Q2_Throughput_WindowAM : "+ meter.getRate()+", ";

                if(i==0)
                    result.append(", "+" AM: ").append(countListAM.get(i).f0);
                else
                    result.append(", ").append(countListAM.get(i).f0);
            }


            for (int i = 0; i < 3 && i < sizePM; i++){
                this.meter.markEvent();
                res2 = " Q1_Throughput_WindowPM : "+ meter.getRate()+", ";

                if(i==0)
                    result.append("; PM: ").append(countListPM.get(i).f0);
                else
                    result.append(", ").append(countListPM.get(i).f0);
            }
            //result.append(res2);
            collector.collect(result.toString());
        }

    }
}
