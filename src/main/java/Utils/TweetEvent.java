package Utils;

import java.util.EventObject;

/**
 * Created by manshu on 4/11/15.
 */
public class TweetEvent extends EventObject {

    private Object data;

    public TweetEvent(Object source) {
        super(source);
        System.out.println("Twitter Object constructor called ");
    }

    public TweetEvent(Object source, Object data) {
        super(source);
        this.data = data;
        //System.out.println("Twitter Object constructor called " + data);
    }

}
