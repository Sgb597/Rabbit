package Pipline.Rabbit;

import java.util.ArrayList;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction 
    extends ProcessWindowFunction<Tuple2<Event, String>, Tramo, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Tuple2<Event, String>> input, Collector<Tramo> out) {
        ArrayList<Event> windowEvents = new ArrayList<Event>();
        Double distanciaInicial = 0.0;
        Double distanciaFinal = 0.0;
        Tramo tramo = new Tramo();

        for (Tuple2<Event, String> in: input) {
            windowEvents.add(in.f0);
        }

        int limit = windowEvents.size();
        int i = 0;
        for (Event e: windowEvents) {
            if (i == 0) {
                distanciaInicial = e.getDistancia();
                tramo.setFechaInicio(e.getFecha());
                tramo.setIdConductor(e.getIdConductor());
                tramo.setIdVehiculo(e.getIdVehiculo());
            }
            if (i == limit) {
                distanciaFinal = e.getDistancia();
                tramo.setFechaFinal(e.getFecha()); 
            }
            i++;
        }
        tramo.setDistance(distanciaInicial, distanciaFinal);
        tramo.setVelocity();

        out.collect(tramo);
    }
}