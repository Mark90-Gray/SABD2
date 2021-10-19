package metrics;

import entity.NYBusLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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

public class Query1metrics {
    //private static final int WINDOW_SIZE = 24;      // giorno
    //private static final int WINDOW_SIZE = 24 * 7;  // settimana
    private static final int WINDOW_SIZE = 24*30;  // mese
    public static void run(DataStream<NYBusLog> stream) throws Exception {
        DataStream<Tuple2<NYBusLog,Long>> timestampedAndWatermarked = stream
                .assignTimestampsAndWatermarks
                        (new BoundedOutOfOrdernessTimestampExtractor<NYBusLog>(Time.seconds(1)) {
                            @Override
                            public long extractTimestamp(NYBusLog logIntegerTuple2) {
                                return logIntegerTuple2.getDateOccuredOn();
                            }
                        }).filter(x -> x.getDelay() != -1)
                .filter(x->!x.getBoro().isEmpty())
                .map(x-> new Tuple2<>(x,System.currentTimeMillis())).
                        returns(Types.TUPLE(Types.GENERIC(NYBusLog.class), Types.LONG));

        DataStream<Tuple3<NYBusLog,String,Long>> prova= timestampedAndWatermarked.map(new RichMapFunction<Tuple2<NYBusLog,Long>, Tuple3<NYBusLog, String,Long>>() {
            private transient Meter meter;

            @Override
            public void open(Configuration parameters) throws Exception {
                com.codahale.metrics.Meter doc= new com.codahale.metrics.Meter();
                this.meter = getRuntimeContext()
                        .getMetricGroup().addGroup("Query1").meter("thro", new DropwizardMeterWrapper(doc));
            }

            @Override
            public Tuple3<NYBusLog, String,Long> map(Tuple2<NYBusLog,Long> nyBusLogLong) throws Exception {
                this.meter.markEvent();
                String res2 = "\n Query_1_throughput_in , " + System.currentTimeMillis() + " , " + meter.getCount() + " , " + meter.getRate();
                return new Tuple3<>(nyBusLogLong.f0, res2,nyBusLogLong.f1);
            }
        });

        //prova.print();

        // somma del delay per boro
        DataStream<String> result_q1m = prova
                .keyBy(value->value.f0.getBoro())
                .timeWindow(Time.hours(WINDOW_SIZE))
                .aggregate( new AverageAggregator(), new KeyBinder())
                .timeWindowAll(Time.hours(WINDOW_SIZE))
                .process(new ResultProcessAllWindows());

        //forse vuole il TextoOutputFormat
        result_q1m.writeAsText(String.format("output"+ "query1Metrics_%d.out",WINDOW_SIZE),
                FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    }

    public static class MyAverage {
        public String boro;
        public Integer count=0;
        public Double sum=0.0;
        public Long ts = 0L;
    }

    private static class AverageAggregator implements AggregateFunction<Tuple3<NYBusLog,String,Long>,
            MyAverage, Tuple2<Double,Long>> {

        @Override
        public MyAverage createAccumulator() {
            return new MyAverage();
        }

        @Override
        public MyAverage add(Tuple3<NYBusLog,String,Long> myNy, MyAverage myAverage) {
            myAverage.boro=myNy.f0.getBoro();
            myAverage.count++;
            myAverage.sum=myAverage.sum+myNy.f0.getDelay();
            myAverage.ts= Math.max(myAverage.ts,myNy.f2);
            return myAverage;
        }

        @Override
        public Tuple2<Double,Long> getResult(MyAverage myAverage) {

            return   new Tuple2<>(myAverage.sum/myAverage.count,myAverage.ts);
        }

        @Override
        public MyAverage merge(MyAverage a, MyAverage b) {
            a.sum+=b.sum;
            a.count+=b.count;

            return a;
        }
    }

    private static class KeyBinder
            extends ProcessWindowFunction<Tuple2<Double,Long>, Tuple2<String, Tuple2<Double,Long>>, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple2<Double,Long>> average,
                            Collector<Tuple2<String, Tuple2<Double,Long>>> out) {
            Tuple2<Double,Long> avg = average.iterator().next();
            out.collect(new Tuple2<>(key, avg));
        }
    }

    private static class ResultProcessAllWindows
            extends ProcessAllWindowFunction<Tuple2<String, Tuple2<Double,Long>>, String, TimeWindow> {
        private transient Meter meter;

        @Override
        public void open(Configuration parameters) throws Exception {
            com.codahale.metrics.Meter doc= new com.codahale.metrics.Meter();
            this.meter = getRuntimeContext()
                    .getMetricGroup().addGroup("Query1").meter("thro", new DropwizardMeterWrapper(doc));
        }



        @Override
        public void process(Context context, Iterable<Tuple2<String, Tuple2<Double,Long>>> iterable, Collector<String> collector) {
            List<Tuple2<String, Tuple2<Double,Long>>> averageList = new ArrayList<>();
            boolean first=true;
            Tuple3<String,Double,Long> max_tuple=null;
            for (Tuple2<String, Tuple2<Double,Long>> t : iterable){
                if (first) {
                    max_tuple = new Tuple3<String, Double, Long>(t.f0,t.f1.f0,t.f1.f1);
                    first=false;
                }
                else if(t.f1.f1 > max_tuple.f2) {
                    max_tuple = new Tuple3<String, Double, Long>(t.f0,t.f1.f0,t.f1.f1);
                }
                averageList.add(t);

            }
            averageList.sort((a, b) -> new Double(b.f1.f0 - a.f1.f0).intValue());

            //StringBuilder result = new StringBuilder(Long.toString(context.window().getStart() /1000));
            LocalDateTime startDate = LocalDateTime.ofEpochSecond(
                    context.window().getStart() / 1000, 0, ZoneOffset.UTC);

            StringBuilder result = new StringBuilder(startDate.toString()+"; ");
            String res2 = " ";
            Long localTime = System.currentTimeMillis();
            this.meter.markEvent();
            //res2 = ";"+ meter.getRate()+";";
            result.append((localTime - max_tuple.f2));

            /*int size = averageList.size();
            for (int i = 0; i < size; i++) {
                this.meter.markEvent();

                res2 = " Q1_Throughput_Window : "+ meter.getRate()+", ";
                result.append(", ").append(averageList.get(i).f0).append(", ").append(averageList.get(i).f1.f0);
                //res2=" ";
            }*/
            //result.append(res2).append(" Latency_Window: "+(localTime - max_tuple.f2));
            collector.collect(result.toString());
        }

    }
}
