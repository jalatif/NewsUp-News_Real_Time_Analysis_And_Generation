package storm;

/**
 * Created by manshu on 3/28/15.
 */

import SentimentAnalysis.SentimentAnalyzer;
import Utils.TweetContent;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import Utils.Rankings;
import Utils.Rankable;

import java.util.*;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

/**
 * A bolt that prints the word and count to redis
 */
public class DumpBolt extends BaseRichBolt
{
    // place holder to keep the connection to redis
    transient RedisConnection<String,String> redis;
    private String[] skipWords = {"http://", "https://", "(", "a", "an", "the", "for", "retweet", "RETWEET", "follow", "FOLLOW"};
    private Rankings last_seen_final_rankings;
    private final String DELIMITER = TopNScoreBolt.DELIMITER;
    private Set<String> topTweetSet;

    @Override
    public void prepare(
            Map                     map,
            TopologyContext         topologyContext,
            OutputCollector         outputCollector)
    {
        // instantiate a redis connection
        RedisClient client = new RedisClient("localhost", 6379);

        // initiate the actual connection
        redis = client.connect();
        topTweetSet = new HashSet<>();
    }

    @Override
    public void execute(Tuple tuple)
    {
        try {
            String boltId = tuple.getSourceComponent(); // get the source of the tweet
            if (boltId.equals("tweet-spout") && last_seen_final_rankings != null) {
                String tweet_info = tuple.getStringByField("tweet-content"); // get the tweet
                TweetContent tweetContent = new TweetContent(tweet_info);
                Integer sentiment_value = SentimentAnalyzer.findSentiment(tweetContent.getTweet());
                // provide the delimiters for splitting the tweet
                String delims = "[ .,?!]+";
                // now split the tweet into tokens
                String[] tokens = tweetContent.getTweet().split(delims);
                // for each token/word, emit it

                for (String word : tokens) {
                    // check if number of words in a tweet is > 3 otherwise don't emit
                    if (word.length() > 3 && !Arrays.asList(skipWords).contains(word)) {

                        if (word.startsWith("#")) { // a hash tag
                            List<Rankable> ranks = last_seen_final_rankings.getRankings();

                            for (Rankable r : ranks) {
                                // access the first column 'word'
                                String hash_word = (String) r.getObject();//tuple.getStringByField("word");

                                // access the second column 'count'
                                Long count = r.getCount();
                                //Integer count = tuple.getIntegerByField("count");

                                //redis.publish("WordCountTopology", word + "*****|" + Long.toString(30L));
                                if (word.equals(hash_word)) {
                                    if (topTweetSet.contains(tweetContent.getTweet())) break;

                                    redis.publish("WordCountTopology", "sen-" + String.valueOf(sentiment_value) + tweetContent.getTweet() +
                                            sentiment_value + "|" + Long.toString(count));

                                    topTweetSet.add(tweetContent.getTweet());
                                    break;
                                }
                            }
                        }
                    }
                }

            } else if (boltId.equals("total-rankings-bolt")) {
                // Save the last seen final top N hash tags so that can be compared with tweets hashes.
                last_seen_final_rankings = (Rankings) tuple.getValue(0);
            } else if (boltId.equals("top-score-bolt")) {
                String top_tweets_list = tuple.getStringByField("top-tweets");
                System.out.println("Jalatif - " + top_tweets_list);

                String[] top_tweets = top_tweets_list.split(DELIMITER);
                for (String top_tweet : top_tweets) {
                    System.out.println("Manika = " + top_tweet);
                    TweetContent tweetContent = new TweetContent(top_tweet);
                    System.out.println("Manu = " + tweetContent.toString());
                    Integer sentiment_value = SentimentAnalyzer.findSentiment(tweetContent.getTweet());
                    //if (topTweetSet.contains(tweetContent.getTweet())) continue;
                    redis.publish("WordCountTopology", tweetContent.getTweet() + "-sen_" +
                            sentiment_value + "|" + tweetContent.getScore().toString());
                    //topTweetSet.add(tweetContent.getTweet());
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }
//        Rankings finalRankings = (Rankings) tuple.getValue(0);
//        List<Rankable> ranks = finalRankings.getRankings();
//        for (Rankable r : ranks) {
//
//
//            // access the first column 'word'
//            String word = (String) r.getObject();//tuple.getStringByField("word");
//
//            // access the second column 'count'
//            Long count = r.getCount();
//            //Integer count = tuple.getIntegerByField("count");
//
//            // publish the word count to redis using word as the key
//            redis.publish("WordCountTopology", word + "|" + Long.toString(count));
//
//        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // nothing to add - since it is the final bolt
    }
}

