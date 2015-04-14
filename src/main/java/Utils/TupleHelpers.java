package Utils;

/**
 * Created by manshu on 3/28/15.
 */
import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

public final class TupleHelpers {

    private TupleHelpers() {
    }

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
                Constants.SYSTEM_TICK_STREAM_ID);
    }

}

