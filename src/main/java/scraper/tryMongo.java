package scraper;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Created by manshu on 4/27/15.
 */
public class tryMongo {

    public static void maien(String[] args) throws UnknownHostException {
        MongoClient mongo = new MongoClient( "localhost" , 27017 );
        List<String> dbs = mongo.getDatabaseNames();
        for(String db : dbs){
            System.out.println(db);
        }
        //DB db = mongo.getDB("local");
        DB db = mongo.getDB("local");
        DBCollection table = db.getCollection("my_test");

        Set<String> tables = db.getCollectionNames();

        for(String coll : tables){
            System.out.println(coll);
        }

        // insert example
        BasicDBObject document = new BasicDBObject();
        document.put("name", "ABHINAV");
        document.put("age", 30);
        document.put("createdDate", new Date());
        table.insert(document);


        // update example
        BasicDBObject query = new BasicDBObject();
        query.put("name", "abhinav");

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put("name", "abhinav-updated");

        BasicDBObject updateObj = new BasicDBObject();
        updateObj.put("$set", newDocument);

        table.update(query, updateObj);


        // find example
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("name", "ABHINAV");

        DBCursor cursor = table.find(searchQuery);

        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

//        // delete example
//        BasicDBObject searchQuery = new BasicDBObject();
//        searchQuery.put("name", "ABHINAV");
//
//        table.remove(searchQuery);


    }

}
