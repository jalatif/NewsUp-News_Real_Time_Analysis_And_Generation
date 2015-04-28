package storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by manshu on 3/23/15.
 */
public class NewsTopology {
    public final static int TOP_N = 50;
    private final static int SUMMARY_MIN_CHARS = 20;
    private final static int MIN_IMAGE_DIMENSIONS_X = 250, MIN_IMAGE_DIMENSIONS_Y = 250;
    private final static String[] NEWS_PAGES = {"bbcworld", "nytimes", "ibnlive"};//"jalatifabhi", "itzsaikat4u"};
    private final static String IMAGE_STORE_PATH = "/home/manshu/Pictures/news";
    private final static String DB_NAME = "local", COLLECTION_NAME = "TweetStore", HOST_NAME = "localhost";
    private final static int HOST_PORT = 27017;

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder topology = new TopologyBuilder();
//        TweetSpout tweetSpout = new TweetSpout(
//                "U5qRMuLtVHY5Fvf5vr2r9pfRH",
//                "MDO4ZapZzzRPj3poFwarUE2wH8WhNlkDzuNfA34LOvtP4b18wT",
//                "144279822-l0Bb4Z2j3uKREd3RUttp4RFv2QcLR2ZN4hbocvlU",
//                "frH1WZGTstlElZvqMA7rOd3Pwpf9cH7q02KSazvZUHgGO"
//        );

        TweetSpout tweetSpout = new TweetSpout(
                Arrays.asList(NEWS_PAGES),
                "U5qRMuLtVHY5Fvf5vr2r9pfRH",
                "MDO4ZapZzzRPj3poFwarUE2wH8WhNlkDzuNfA34LOvtP4b18wT",
                "144279822-l0Bb4Z2j3uKREd3RUttp4RFv2QcLR2ZN4hbocvlU",
                "frH1WZGTstlElZvqMA7rOd3Pwpf9cH7q02KSazvZUHgGO"
        );

        // set spout as tweet spout using parallelism of 1
        topology.setSpout("tweet-spout", tweetSpout, 1);

        topology.setBolt("filter-bolt", new FilterBolt(), 4).shuffleGrouping("tweet-spout");

        topology.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 4).shuffleGrouping("filter-bolt");

        topology.setBolt("comment-bolt", new CommentsScraperBolt(), 4).shuffleGrouping("filter-bolt");

        topology.setBolt("sentiment-bolt", new SentimentBolt(), 4).fieldsGrouping("comment-bolt", new Fields("tweet-id")).
                shuffleGrouping("filter-bolt");

        topology.setBolt("emoticon-bolt", new EmoticonBolt(), 4).fieldsGrouping("comment-bolt", new Fields("tweet-id")).
                shuffleGrouping("filter-bolt");

        topology.setBolt("summary-bolt", new SummaryTweetBolt(SUMMARY_MIN_CHARS), 6).shuffleGrouping("filter-bolt");

        topology.setBolt("image-collection-bolt", new ImageCollectionBolt(MIN_IMAGE_DIMENSIONS_X, MIN_IMAGE_DIMENSIONS_Y,
                IMAGE_STORE_PATH), 3).shuffleGrouping("filter-bolt");

        topology.setBolt("final-bolt", new FinalBolt(DB_NAME, COLLECTION_NAME, HOST_NAME, HOST_PORT), 10).
                fieldsGrouping("filter-bolt", new Fields("tweet-id")).
                    fieldsGrouping("parse-tweet-bolt", new Fields("tweet-id")).
                        fieldsGrouping("comment-bolt", new Fields("tweet-id")).
                            fieldsGrouping("sentiment-bolt", new Fields("tweet-id")).
                                fieldsGrouping("emoticon-bolt", new Fields("tweet-id")).
                                    fieldsGrouping("summary-bolt", new Fields("tweet-id")).
                                        fieldsGrouping("image-collection-bolt", new Fields("tweet-id"));


//        // Ranking based on count and most popular hashtags
//
//        // #hashtag based
//        topology.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));
//
//        topology.setBolt("intermediate-rankings-bolt", new IntermediateRankingsBolt(TOP_N), 4).
//                fieldsGrouping("count-bolt", new Fields("word"));
//
//        topology.setBolt("total-rankings-bolt", new TotalRankingsBolt(TOP_N), 1).globalGrouping("intermediate-rankings-bolt");
//
//        // attach rolling count bolt using fields grouping - parallelism of 5
//        // builder.setBolt("rolling-count-bolt", new RollingCountBolt(30, 10), 1).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));
//
//        topology.setBolt("top-score-bolt", new TopNScoreBolt(TOP_N), 1).shuffleGrouping("filter-bolt");
//
//        // attach the report bolt using global grouping - parallelism of 1
//        topology.setBolt("dump-bolt", new DumpBolt(), 1).globalGrouping("top-score-bolt").
//                globalGrouping("total-rankings-bolt").globalGrouping("tweet-spout");


        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            // run it in a live cluster
            // set the number of workers for running all spout and bolt tasks
            conf.setNumWorkers(3);
            // submit the topology
            StormSubmitter.submitTopology(args[0], conf, topology.createTopology());

        } else {
            // run it in a simulated local cluster
            // set the number of threads to run - similar to setting number of workers in live cluster
            conf.setMaxTaskParallelism(4);

            LocalCluster cluster = new LocalCluster();

            // submit the topology to the local cluster
            cluster.submitTopology("uiuc-news-analysis", conf, topology.createTopology());

            // let the topology run for 300 seconds
            Utils.sleep(300 * 1000000);

            // now kill the topology
            cluster.killTopology("uiuc-news-analysis");

            // we are done, so shutdown the local cluster
            cluster.shutdown();
        }
    }
}
