package scraper;

import java.util.EventObject;

/**
 * Created by manshu on 4/6/15.
 */
public class TwitterChangeEvent extends EventObject {

    public Object data;

    public TwitterChangeEvent(Object source) {
        super(source);
        System.out.println("Twitter Object constructor called");
    }

    public TwitterChangeEvent(Object source, Object data) {
        super(source);
        System.out.println("Twitter Object constructor called " + data);
        this.data = data;
    }

}
