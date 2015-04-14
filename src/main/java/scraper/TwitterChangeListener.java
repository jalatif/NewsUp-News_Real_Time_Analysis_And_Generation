package scraper;

import java.util.EventObject;

/**
 * Created by manshu on 4/6/15.
 */
public interface TwitterChangeListener {
    public void handleTwitterChangeListener(EventObject eventObject, Object data);
}
