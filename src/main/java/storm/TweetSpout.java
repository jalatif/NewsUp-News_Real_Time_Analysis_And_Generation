package storm;

import SentimentAnalysis.SentimentAnalyzer;
import Utils.TweetContent;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.EventObject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import Utils.TwitterPageUpdateListener;
import Utils.TwitterNewsStream;
import Utils.NewsGenerator;

/**
 * Created by manshu on 3/22/15.
 */
public class TweetSpout extends BaseRichSpout {

    String customer_key, customer_secret;
    String access_token, access_secret;
    List<String> news_pages;

    SpoutOutputCollector spoutOutputCollector;

    TwitterStream twitterStream;
    Twitter twitter;

    LinkedBlockingQueue<String> queue = null;

    Pattern moodPattern = Pattern.compile("love|hate|happy|angry|sad");
    Pattern properPattern = Pattern.compile("^[a-zA-Z0-9 ]+$");

    public TweetSpout(String key, String secret, String token, String tokensecret) {
        this.news_pages = null;
        customer_key = key;
        customer_secret = secret;
        access_token = token;
        access_secret = tokensecret;
    }

    public TweetSpout(List<String> news_pages, String key, String secret, String token, String tokensecret) {
        this.news_pages = news_pages;
        customer_key = key;
        customer_secret = secret;
        access_token = token;
        access_secret = tokensecret;
    }

    private class TweetUpdateListener implements TwitterPageUpdateListener {
        @Override
        public void onPageUpdate(EventObject eventObject, Object data) {
            //System.out.println("I am handling it from source of " + eventObject.getSource() + " with data of " + data);
            try {
                TweetContent tweetContent = new TweetContent((Status) data);
                queue.offer(tweetContent.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private class TweetListener implements StatusListener {
        @Override
        public void onStatus(Status status) {
            // add the tweet into the queue buffer
            try {
                TweetContent tweetContent = new TweetContent(status);
                queue.offer(tweetContent.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

        }

        @Override
        public void onTrackLimitationNotice(int i) {

        }

        @Override
        public void onScrubGeo(long l, long l1) {

        }

        @Override
        public void onStallWarning(StallWarning stallWarning) {

        }

        @Override
        public void onException(Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-content"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.queue = new LinkedBlockingQueue<String>(1000); // buffer to block tweets
        SentimentAnalyzer.init();
        this.spoutOutputCollector = collector;

//        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
//        configurationBuilder.setOAuthConsumerKey(this.customer_key);
//        configurationBuilder.setOAuthConsumerSecret(this.customer_secret);
//        configurationBuilder.setOAuthAccessToken(this.access_token);
//        configurationBuilder.setOAuthAccessTokenSecret(this.access_secret);
//
//        this.twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
//        ConfigurationBuilder cb = new ConfigurationBuilder();
//        cb.setDebugEnabled(true)
//                .setOAuthConsumerKey(customer_key)
//                .setOAuthConsumerSecret(customer_secret)
//                .setOAuthAccessToken(access_token)
//                .setOAuthAccessTokenSecret(access_secret);
//        this.twitter = new TwitterFactory(cb.build()).getInstance();
//
//        FilterQuery filterQuery = new FilterQuery();
//
//        filterQuery.language(new String[]{"en"});
//        filterQuery.locations(new double[][]{
//                new double[]{-133.6144013405, -50.3163235209},
//                new double[]{-41.8907623291, 59.0253526528}
//        });
//
//        this.twitterStream.addListener(new TweetListener());
//        this.twitterStream.filter(filterQuery);
//
//        // start sampling of twitter tweets
//        twitterStream.sample();

        TwitterNewsStream twitterNewsSource = new TwitterNewsStream();

        twitterNewsSource.addEventListener(new TweetUpdateListener());

        NewsGenerator newsGenerator = new NewsGenerator(news_pages, twitterNewsSource);
        newsGenerator.sample();
    }

    @Override
    public void nextTuple() {
        String tweet_info = queue.poll();

        if (tweet_info == null) {
            Utils.sleep(50);
            return;
        } else {
//            String user = "bbcworld";
//            List<Status> statuses = null;
//            try {
//                statuses = twitter.getUserTimeline(user);
//                for (Status status1: statuses) {
//                    TweetContent tweetContent = new TweetContent(status1);
//                    //queue.offer(tweetContent.toString());
//                    //int sentiment_value = SentimentAnalyzer.findSentiment(tweetContent.getTweet());
//                    //System.out.println("Tweet = " + tweetContent.toString());
//                    spoutOutputCollector.emit(new Values(tweetContent.toString()));
//                    Utils.sleep(500);
//                }
//            } catch (TwitterException e) {
//                e.printStackTrace();
//            }
            TweetContent tweetContent = new TweetContent(tweet_info);
            //int sentiment_value = SentimentAnalyzer.findSentiment(tweetContent.getTweet());
            System.out.println(" Tweet = " + tweetContent.toString());
            spoutOutputCollector.emit(new Values(tweetContent.toString()));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.setMaxTaskParallelism(1);
        return config;
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }
}
