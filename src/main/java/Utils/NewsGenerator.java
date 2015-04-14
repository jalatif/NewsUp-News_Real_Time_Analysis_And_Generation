package Utils;

import org.apache.xml.serializer.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by manshu on 4/11/15.
 */
public class NewsGenerator {
    private List<String> news_pages;
    private TwitterNewsStream stream;
    private Thread[] pageThreads;
    private LinkedBlockingDeque<Status> statusQueue;
    private Twitter twitter;

    private volatile boolean running;
    private final double PAGEUPDATEINTERVAL = 3.0;
    private final double EMITFREQUENCY = 0.5;

    String consumerKey="U5qRMuLtVHY5Fvf5vr2r9pfRH";
    String consumerSecret="MDO4ZapZzzRPj3poFwarUE2wH8WhNlkDzuNfA34LOvtP4b18wT";
    String accessToken="144279822-l0Bb4Z2j3uKREd3RUttp4RFv2QcLR2ZN4hbocvlU";
    String accessSecret="frH1WZGTstlElZvqMA7rOd3Pwpf9cH7q02KSazvZUHgGO";


    private void init() {
        pageThreads = new Thread[news_pages.size()];
        running = true;
        statusQueue = new LinkedBlockingDeque<>(1000);

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(consumerKey);
        configurationBuilder.setOAuthConsumerSecret(consumerSecret);
        configurationBuilder.setOAuthAccessToken(accessToken);
        configurationBuilder.setOAuthAccessTokenSecret(accessSecret);

        twitter = new TwitterFactory(configurationBuilder.build()).getInstance();

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                emit();
            }
        }, 0, (long) (EMITFREQUENCY * 1000));
    }

    public NewsGenerator(List<String> news_pages, TwitterNewsStream stream) {
        this.news_pages = news_pages;
        this.stream = stream;
        init();
    }

    public NewsGenerator() {
        news_pages = new ArrayList<>();
        stream = null;
        news_pages.add("jalatifabhi");
        news_pages.add("itzsaikat4u");
        init();
    }

    public void shutDown() {
        running = false;
    }

    public void emit() {
        Status tweet_status = statusQueue.poll();
        if (tweet_status != null) {
            TweetContent tweetContent = new TweetContent(tweet_status);
            if (stream == null) {
                System.out.println(tweetContent.toString());
            } else {
                stream.fireEvent(tweet_status);
            }

        }
    }

    public void sample() {
        int i = 0;
        for (String page : news_pages) {
            pageThreads[i] = new Thread(() -> {
                pageCheck(page);
            });
            pageThreads[i].start();
        }
    }


    public void pageCheck(String page) {
        List<Status> statuses = new ArrayList<>();
        int page_num = 1;
        Paging paging = new Paging(page_num, 50);
        long sinceId = -1, prev_since_id = -1;
        try {
            while (statuses.isEmpty() && running) {
                statuses = twitter.getUserTimeline(page, paging);
                while (!statuses.isEmpty()) {
                    long maxId = -1;
                    boolean endNext = false;
                    for (Status status : statuses) {
                        if (status.getId() < prev_since_id) {
                            endNext = true;
                            break;
                        }
                        statusQueue.offer(status);
//                                    System.out.println("*******************************");
//                                    System.out.print("ID = " + status.getId());
//                                    System.out.println(" Text = " + status.getText() + " Date = " + status.getCreatedAt().toString());
//
//                                    System.out.println("*******************************\n");
                        maxId = Math.max(maxId, status.getId());
                        sinceId = Math.max(sinceId, status.getId());
                    }
                    if (endNext) break;
                    paging.setMaxId(maxId);
                    //System.out.println("MaxId = " + paging.getMaxId());
                    paging.setPage(++page_num);

//                                System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
//                                System.out.println();
                    Thread.sleep((int) (PAGEUPDATEINTERVAL) * 1000);
                    statuses = twitter.getUserTimeline(page, paging);
                }
                page_num = 1;
                prev_since_id = sinceId;
                //System.out.println(sinceId);
                if (sinceId != -1) {
                    paging = new Paging(sinceId);
                } else {
                    paging = new Paging(page_num, 10);
                }
                statuses = Collections.EMPTY_LIST;
                Thread.sleep((int) PAGEUPDATEINTERVAL * 1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to get timeline: " + e.getMessage());
            System.out.println("Stopping thread of " + page);
        }
    }

    public static void main(String[] args) {
        NewsGenerator news = new NewsGenerator();
        news.sample();
    }
}
