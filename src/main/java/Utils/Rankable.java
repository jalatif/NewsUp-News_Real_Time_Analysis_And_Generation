package Utils;

/**
 * Created by manshu on 3/28/15.
 */

public interface Rankable extends Comparable<Rankable> {

    Object getObject();

    long getCount();

    /**
     * Note: We do not defensively copy the object wrapped by the Rankable.  It is passed as is.
     *
     * @return a defensive copy
     */
    Rankable copy();
}

