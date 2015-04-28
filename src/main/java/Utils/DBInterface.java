package Utils;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Date;

/**
 * Created by manshu on 4/28/15.
 */
public class DBInterface {
    private static MongoClient mongo;
    private static DB db;
    private static DBCollection table;

    public static void init(String db_name, String collection_name, String host_location, int port_num) {
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

    public static void updateLocation(Long tweet_id, double longitude, double latitude) {
        BasicDBObject locationObject = new BasicDBObject();
        locationObject.put("lon", longitude);
        locationObject.put("lat", latitude);
        updateVal(tweet_id, "location", locationObject);
    }

    public static boolean insertNewTweet(Long tweetId) {
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

    public static void updateVal(Long tweetId, String key, Object val) {
        BasicDBObject query = new BasicDBObject();
        query.put("tweet_id", tweetId);

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put("$set", new BasicDBObject(key, val));

        table.update(query, newDocument);
    }

    public static void updateList(Long tweetId, String column, Object value) {
        BasicDBObject oldFindquery = new BasicDBObject();
        oldFindquery.put("tweet_id", tweetId);

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put("$push", new BasicDBObject(column, value));

        table.update(oldFindquery, newDocument);
    }

    public static void updateDocumentInList(Long tweetId, int commentId, String key, Object val) {
        BasicDBObject oldFindquery = new BasicDBObject();
        oldFindquery.put("tweet_id", tweetId);
        oldFindquery.put("comments-score.id", commentId);
        BasicDBObject newDocument = new BasicDBObject();

        newDocument.put("$set", new BasicDBObject("comments-score.$." + key, val));

        table.update(oldFindquery, newDocument);
    }

    public static boolean insertNewCommentDocument(Long tweetId, int commentId) {
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
        updateList(tweetId, "comments-score", comment_document);

        return true;
    }

    public static void main(String[] args) throws UnknownHostException {
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        DB db = mongoClient.getDB("local");
        DBCollection table = db.getCollection("TweetStore");
    }

}
