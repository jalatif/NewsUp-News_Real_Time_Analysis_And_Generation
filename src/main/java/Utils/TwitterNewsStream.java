package Utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by manshu on 4/11/15.
 */
public class TwitterNewsStream {
    private List _listeners;

    public synchronized void addEventListener(TwitterPageUpdateListener listener)  {
        _listeners.add(listener);
    }
    public synchronized void removeEventListener(TwitterPageUpdateListener listener)   {
        _listeners.remove(listener);
    }

    public synchronized void fireEvent(Object data) {
        TweetEvent event = new TweetEvent(this, data);
        Iterator<TwitterPageUpdateListener> i = _listeners.iterator();
        while(i.hasNext())  {
            i.next().onPageUpdate(event, data);
        }
    }

    public TwitterNewsStream() {
        _listeners = new ArrayList<>();
    }

}
