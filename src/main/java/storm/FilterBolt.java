package storm;

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
 * Created by manshu on 3/24/15.
 */
public class FilterBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String tweet_info = input.getStringByField("tweet-content");
        TweetContent tweetContent = new TweetContent(tweet_info);

        if (tweetContent.getUrls().size() == 0) {
            System.out.println("This is not accepted." + tweetContent.toString());
            return;
        }

        int score = tweetContent.getFavorites() + tweetContent.getRetweets();
        score = Math.max(5, score);
        score = Math.min(100, score);
        tweetContent.setScore(score);
        System.out.println("Jalatif " + tweetContent.getUrls().get(0) + " -> " + tweetContent.toString());
        outputCollector.emit(new Values(tweetContent.getId(), tweetContent.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-id", "tweet-content"));
    }
}
