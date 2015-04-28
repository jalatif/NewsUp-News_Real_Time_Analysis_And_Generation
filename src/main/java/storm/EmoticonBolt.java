package storm;

/**
 * Created by manshu on 4/27/15.
 */

import SentimentAnalysis.EmoticonAnalyzer;
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
public class EmoticonBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private EmoticonAnalyzer emoticonAnalyzer;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        emoticonAnalyzer = new EmoticonAnalyzer();
    }

    @Override
    public void execute(Tuple input) {
        String boltId = input.getSourceComponent();
        if (boltId.equals("comment-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            Integer comment_id = input.getIntegerByField("comment-id");
            String comment = input.getStringByField("comment");
            int emoticon = emoticonAnalyzer.getEmoticonScore(comment);
            System.out.println("Emoticon = " + emoticon + " tweet-id=" + tweet_id + " for comment = " + comment);
            outputCollector.emit(new Values(tweet_id, comment_id, emoticon));
        } else if (boltId.equals("filter-bolt")) {
            String tweet_info = input.getStringByField("tweet-content");
            TweetContent tweetContent = new TweetContent(tweet_info);
            int emoticon = emoticonAnalyzer.getEmoticonScore(tweetContent.getTweet());
            System.out.println("Emoticon = " + emoticon + " tweet-id=" + tweetContent.getId());
            outputCollector.emit(new Values(tweetContent.getId(), 0, emoticon));//0 is for original tweet
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-id", "comment-id", "emoticon"));
    }
}

