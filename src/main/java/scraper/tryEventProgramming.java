package scraper;

/**
 * Created by manshu on 4/6/15.
 */
public class tryEventProgramming {
    public static void main(String[] args) {
        MyTwitterListener tl1 = new MyTwitterListener();
        MyTwitterListener tl2= new MyTwitterListener();

        TwitterChangeSource tcs = new TwitterChangeSource();
        tcs.addEventListener(tl1);
        tcs.addEventListener(tl2);

        for (int i = 0; i < 10; i++) {
            tcs.fireEvent(i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
