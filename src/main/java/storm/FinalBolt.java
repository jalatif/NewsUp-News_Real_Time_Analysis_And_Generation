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

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        try {
            mongo = new MongoClient( "localhost" , 27017 );
            db = mongo.getDB("local");
            table = db.getCollection("TweetStore");
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.out.println("Cannot make db connection");
            System.exit(0);
        }
    }

    @Override
    public void execute(Tuple input) {
        String boltId = input.getSourceComponent();
        switch (boltId) {
            case "filter-bolt":
                break;
            case "comment-bolt":
                break;
            case "sentiment-bolt":
                break;
            case "emoticon-bolt":
                break;
            case "summary-bolt":
                break;
            case "image-collection-bolt":
            default:
                break;
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
