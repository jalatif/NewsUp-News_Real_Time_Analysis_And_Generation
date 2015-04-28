package storm;

import SentimentAnalysis.SentimentAnalyzer;
import Utils.TweetContent;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by manshu on 4/27/15.
 */
public class SentimentBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String boltId = input.getSourceComponent();
        if (boltId.equals("comment-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            Integer comment_id = input.getIntegerByField("comment-id");
            String comment = input.getStringByField("comment");
            int sentiment = SentimentAnalyzer.findSentiment(comment);
            System.out.println("Sentiment = " + sentiment + " tweet-id=" + tweet_id + " for comment = " + comment);
            outputCollector.emit(new Values(tweet_id, comment_id, sentiment));

        } else if (boltId.equals("filter-bolt")) {
            String tweet_info = input.getStringByField("tweet-content");
            TweetContent tweetContent = new TweetContent(tweet_info);
            int sentiment = SentimentAnalyzer.findSentiment(tweetContent.getTweet());
            System.out.println("Sentiment = " + sentiment + " tweet-id=" + tweetContent.getId());
            outputCollector.emit(new Values(tweetContent.getId(), 0, sentiment)); //0 is for original tweet
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-id", "comment-id", "sentiment"));
    }
}
