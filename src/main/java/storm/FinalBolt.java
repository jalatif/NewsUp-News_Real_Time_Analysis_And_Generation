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
import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

/**
 * Created by manshu on 4/27/15.
 */

public class FinalBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private MongoClient mongo;
    private DB db;
    private DBCollection table;

    public FinalBolt(String db_name, String collection_name, String host_location, int port_num) {
        try {
            mongo = new MongoClient(host_location, port_num);
            db = mongo.getDB(db_name);
            table = db.getCollection(collection_name);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.out.println("Cannot make db connection");
            System.exit(1);
        }
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
            insertNewTweet(table, tweet_id);

            updateVal(table, tweet_id, "tweet_content", tweetContent.getTweet());

            for (String url : tweetContent.getUrls()) {
                updateVal(table, tweet_id, "tweet_urls", url);
            }

            updateVal(table, tweet_id, "source", tweetContent.getUser());

            updateVal(table, tweet_id, "favorite-count", tweetContent.getFavorites());

            updateVal(table, tweet_id, "retweet-count", tweetContent.getRetweets());

            BasicDBObject locationObject = new BasicDBObject();
            locationObject.put("lon", tweetContent.getLongitude());
            locationObject.put("lat", tweetContent.getLatitude());
            updateVal(table, tweet_id, "location", locationObject);

            updateVal(table, tweet_id, "time_posted", tweetContent.getTime());
        }

        else if (boltId.equals("parse-tweet-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            String hashtag = input.getStringByField("tweet-word");
            insertNewTweet(table, tweet_id);
            updateList(table, tweet_id, "hash_tags", hashtag);
        }

        else if (boltId.equals("comment-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            int comment_id = input.getIntegerByField("comment-id");
            String comment = input.getStringByField("comment");
            insertNewTweet(table, tweet_id);
            insertNewCommentDocument(table, tweet_id, comment_id);
            updateDocumentInList(table, tweet_id, comment_id, "comment", comment);
        }

        else if (boltId.equals("sentiment-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            int comment_id = input.getIntegerByField("comment-id");
            int sentiment = input.getIntegerByField("sentiment");
            insertNewTweet(table, tweet_id);
            if (comment_id == 0) {
                updateVal(table, tweet_id, "sentiment-score", sentiment);
            } else {
                insertNewCommentDocument(table, tweet_id, comment_id);
                updateDocumentInList(table, tweet_id, comment_id, "sentiment", sentiment);
            }
        }

        else if (boltId.equals("emoticon-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            int comment_id = input.getIntegerByField("comment-id");
            int emoticon = input.getIntegerByField("emoticon");
            insertNewTweet(table, tweet_id);
            if (comment_id == 0) {
                updateVal(table, tweet_id, "emoticon-score", emoticon);
            } else {
                insertNewCommentDocument(table, tweet_id, comment_id);
                updateDocumentInList(table, tweet_id, comment_id, "emoticon", emoticon);
            }
        }

        else if (boltId.equals("summary-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            String summary = input.getStringByField("summary");
            insertNewTweet(table, tweet_id);
            updateVal(table, tweet_id, "summary", summary);
        }

        else if (boltId.equals("image-collection-bolt")) {
            Long tweet_id = input.getLongByField("tweet-id");
            String image_url = input.getStringByField("image_url");
            insertNewTweet(table, tweet_id);
            updateList(table, tweet_id, "images", image_url);
        }

        else {

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to add - since it is the final bolt
    }

    private static boolean insertNewTweet(DBCollection table, Long tweetId) {
        BasicDBObject query = new BasicDBObject();
        query.put("tweet_id", tweetId);
        DBCursor cursor = table.find(query);
        if (cursor.size() != 0) return false;

        BasicDBObject document = new BasicDBObject();
        document.put("tweet_id", tweetId);
        document.put("tweet_content", "");
        document.put("hash_tags", new BasicDBList());
        document.put("tweet_urls", new BasicDBList());
        document.put("summary", "");
        document.put("images", new BasicDBList());
        document.put("source", "");
        document.put("comments-score", new BasicDBList());
        document.put("emoticon-score", "");
        document.put("sentiment-score", "");
        document.put("favorite-count", "");
        document.put("retweet-count", "");
        document.put("location", new BasicDBObject("lon", 0.0).append("lat", 0.0));
        document.put("language", "en");
        document.put("time_posted", new Date());
        table.insert(document);

        return true;
    }

    private static void updateVal(DBCollection table, Long tweetId, String key, Object val) {
        BasicDBObject query = new BasicDBObject();
        query.put("tweet_id", tweetId);

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put("$set", new BasicDBObject(key, val));

        table.update(query, newDocument);
    }

    private static void updateList(DBCollection table, Long tweetId, String column, Object value) {
        BasicDBObject oldFindquery = new BasicDBObject();
        oldFindquery.put("tweet_id", tweetId);

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put("$push", new BasicDBObject(column, value));

        table.update(oldFindquery, newDocument);
    }

    private static void updateDocumentInList(DBCollection table, Long tweetId, int commentId, String key, Object val) {
        BasicDBObject oldFindquery = new BasicDBObject();
        oldFindquery.put("tweet_id", tweetId);
        oldFindquery.put("comments-score.id", commentId);
        BasicDBObject newDocument = new BasicDBObject();

        newDocument.put("$set", new BasicDBObject("comments-score.$." + key, val));

        table.update(oldFindquery, newDocument);
    }

    private static boolean insertNewCommentDocument(DBCollection table, Long tweetId, int commentId) {
        BasicDBObject query = new BasicDBObject();
        BasicDBList list = new BasicDBList();
        list.add(new BasicDBObject("tweet_id", tweetId));
        list.add(new BasicDBObject("comments-score.id", commentId));//, new BasicDBObject("$elemMatch", new BasicDBObject("id", 2))));
        query.put("$and", list);
        DBCursor cursor = table.find(query, new BasicDBObject("comments-score", 1).append("_id", 0));
        if (cursor.size() != 0) return false;

        BasicDBObject comment_document = new BasicDBObject();
        comment_document.put("id", commentId);
        comment_document.put("comment", "");
        comment_document.put("sentiment", "");
        comment_document.put("emoticon", "");
        updateList(table, tweetId, "comments-score", comment_document);

        return true;
    }

    public static void main(String[] args) throws UnknownHostException {
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        DB db = mongoClient.getDB("local");
        DBCollection table = db.getCollection("TweetStore");
    }
}
