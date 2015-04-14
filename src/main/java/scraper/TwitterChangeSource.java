package scraper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by manshu on 4/6/15.
 */
public class TwitterChangeSource {
    private List _listeners = new ArrayList<>();
    public synchronized void addEventListener(TwitterChangeListener listener)  {
        _listeners.add(listener);
    }
    public synchronized void removeEventListener(TwitterChangeListener listener)   {
        _listeners.remove(listener);
    }

    // call this method whenever you want to notify
    //the event listeners of the particular event
    public synchronized void fireEvent(Object data) {
        TwitterChangeEvent event = new TwitterChangeEvent(this, data);
        Iterator i = _listeners.iterator();
        while(i.hasNext())  {
            ((TwitterChangeListener) i.next()).handleTwitterChangeListener(event, data);
        }
    }
}
