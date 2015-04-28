package storm;

import SentimentAnalysis.SentimentAnalyzer;
import Utils.DBInterface;
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

public class FinalBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    public FinalBolt(String db_name, String collection_name, String host_location, int port_num) {
        DBInterface.init(db_name, collection_name, host_location, port_num);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String boltId = input.getSourceComponent();
        if (boltId.equals("filter-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            String tweet_info = input.getStringByField("tweet-content");
            TweetContent tweetContent = new TweetContent(tweet_info);
            System.out.println("Final filter = " + tweetContent + " tweet-id = " + tweet_id);
            DBInterface.insertNewTweet(tweet_id);

            DBInterface.updateVal(tweet_id, "tweet_content", tweetContent.getTweet());

            for (String url : tweetContent.getUrls()) {
                DBInterface.updateVal(tweet_id, "tweet_urls", url);
            }

            DBInterface.updateVal(tweet_id, "source", tweetContent.getUser());

            DBInterface.updateVal(tweet_id, "favorite-count", tweetContent.getFavorites());

            DBInterface.updateVal(tweet_id, "retweet-count", tweetContent.getRetweets());

            DBInterface.updateLocation(tweet_id, tweetContent.getLongitude(), tweetContent.getLatitude());

            DBInterface.updateVal(tweet_id, "time_posted", tweetContent.getTime());
        }

        else if (boltId.equals("parse-tweet-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            String hashtag = input.getStringByField("tweet-word");
            System.out.println("Final parse = " + hashtag + " tweet-id = " + tweet_id);
            DBInterface.insertNewTweet(tweet_id);
            DBInterface.updateList(tweet_id, "hash_tags", hashtag);
        }

        else if (boltId.equals("comment-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            int comment_id = input.getIntegerByField("comment-id");
            String comment = input.getStringByField("comment");
            System.out.println("Final comment " + comment + " comment id = " + comment_id + " tweet-id = " + tweet_id);
            DBInterface.insertNewTweet(tweet_id);
            DBInterface.insertNewCommentDocument(tweet_id, comment_id);
            DBInterface.updateDocumentInList(tweet_id, comment_id, "comment", comment);
        }

        else if (boltId.equals("sentiment-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            int comment_id = input.getIntegerByField("comment-id");
            int sentiment = input.getIntegerByField("sentiment");
            System.out.println("Final sentiment = " + sentiment + " comment id = " + comment_id +
                    " tweet-id = " + tweet_id);
            DBInterface.insertNewTweet(tweet_id);
            if (comment_id == 0) {
                DBInterface.updateVal(tweet_id, "sentiment-score", sentiment);
            } else {
                DBInterface.insertNewCommentDocument(tweet_id, comment_id);
                DBInterface.updateDocumentInList(tweet_id, comment_id, "sentiment", sentiment);
            }
        }

        else if (boltId.equals("emoticon-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            int comment_id = input.getIntegerByField("comment-id");
            int emoticon = input.getIntegerByField("emoticon");
            System.out.println("Final emoticon = " + emoticon + " comment id = " + comment_id +
                    " tweet-id = " + tweet_id);
            DBInterface.insertNewTweet(tweet_id);
            if (comment_id == 0) {
                DBInterface.updateVal(tweet_id, "emoticon-score", emoticon);
            } else {
                DBInterface.insertNewCommentDocument(tweet_id, comment_id);
                DBInterface.updateDocumentInList(tweet_id, comment_id, "emoticon", emoticon);
            }
        }

        else if (boltId.equals("summary-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            String summary = input.getStringByField("summary");
            System.out.println("Final summary = " + summary + " tweet_id = " + tweet_id);
            DBInterface.insertNewTweet(tweet_id);
            DBInterface.updateVal(tweet_id, "summary", summary);
        }

        else if (boltId.equals("image-collection-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            String image_url = input.getStringByField("image_url");
            System.out.println("Final image_url = " + image_url + " tweet_id = " + tweet_id);
            DBInterface.insertNewTweet(tweet_id);
            DBInterface.updateList(tweet_id, "images", image_url);
        }

        else {

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to add - since it is the final bolt
    }

}
