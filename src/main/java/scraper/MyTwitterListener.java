package scraper;

import java.util.EventObject;

/**
 * Created by manshu on 4/6/15.
 */
public class MyTwitterListener implements TwitterChangeListener {

    @Override
    public void handleTwitterChangeListener(EventObject eventObject, Object data) {
        System.out.println("I am handling it from source of " + eventObject.getSource() + " with data of " + data);
    }

}