package Pipline.Rabbit;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class TramoTrigger<T, W extends Window> extends Trigger<T, W> {
    private static final long serialVersionUID = 1L;

    private final ValueStateDescriptor<Tuple2<JoinedEvent, String>> stateDesc = new ValueStateDescriptor<Tuple2<JoinedEvent, String>>("last-element", TypeInformation.of(new TypeHint<Tuple2<JoinedEvent, String>>() {}));
    
    public TramoTrigger() {}
    
    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
    	
        ValueState<Tuple2<JoinedEvent, String>> lastElementState = ctx.getPartitionedState(stateDesc);
        
        Tuple2<JoinedEvent, String> currentEvent = (Tuple2<JoinedEvent, String>) element;
        String idEstado = currentEvent.f0.getIdEstado();
        
        if (lastElementState.value() == null) {
        	lastElementState.update((Tuple2<JoinedEvent, String>) element);
            return TriggerResult.CONTINUE;
        }
        else if (lastElementState.value() != null) {
            lastElementState.update((Tuple2<JoinedEvent, String>) element);
            if (idEstado.equals("37+") || idEstado.equals("38+")) {
            	lastElementState.update(null);
            	System.out.println("TRIGGER FIRE_AND_PURGE PARADA");
            	return TriggerResult.FIRE_AND_PURGE;
            } else if (idEstado.equals("50+")) {
            	lastElementState.update(null);
            	System.out.println("TRIGGER FIRE_AND_PURGE CAMBIO DE CONDUCTOR");
            	return TriggerResult.FIRE_AND_PURGE;
            } else if (idEstado.equals("243")) {
	        	lastElementState.update(null);
	        	System.out.println("TRIGGER FIRE_AND_PURGE CAMBIO DE PAIS");
	        	return TriggerResult.FIRE_AND_PURGE;
	        }
        } else {
            return TriggerResult.CONTINUE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		// TODO Auto-generated method stub
		
	}
}
