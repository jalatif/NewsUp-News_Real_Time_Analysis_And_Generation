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
    private final static String[] news_pages = {"bbcworld", "jalatifabhi", "itzsaikat4u"};

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder topology = new TopologyBuilder();
//        TweetSpout tweetSpout = new TweetSpout(
//                "U5qRMuLtVHY5Fvf5vr2r9pfRH",
//                "MDO4ZapZzzRPj3poFwarUE2wH8WhNlkDzuNfA34LOvtP4b18wT",
//                "144279822-l0Bb4Z2j3uKREd3RUttp4RFv2QcLR2ZN4hbocvlU",
//                "frH1WZGTstlElZvqMA7rOd3Pwpf9cH7q02KSazvZUHgGO"
//        );

        TweetSpout tweetSpout = new TweetSpout(
                Arrays.asList(news_pages),
                "U5qRMuLtVHY5Fvf5vr2r9pfRH",
                "MDO4ZapZzzRPj3poFwarUE2wH8WhNlkDzuNfA34LOvtP4b18wT",
                "144279822-l0Bb4Z2j3uKREd3RUttp4RFv2QcLR2ZN4hbocvlU",
                "frH1WZGTstlElZvqMA7rOd3Pwpf9cH7q02KSazvZUHgGO"
        );

        // set spout as tweet spout using parallelism of 1
        topology.setSpout("tweet-spout", tweetSpout, 1);

        topology.setBolt("filter-bolt", new FilterBolt(), 4).shuffleGrouping("tweet-spout");

        topology.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 4).shuffleGrouping("filter-bolt");
//
//        // #hashtag based
        topology.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));
//
        topology.setBolt("intermediate-rankings-bolt", new IntermediateRankingsBolt(TOP_N), 4).
                fieldsGrouping("count-bolt", new Fields("word"));
//
        topology.setBolt("total-rankings-bolt", new TotalRankingsBolt(TOP_N), 1).globalGrouping("intermediate-rankings-bolt");

        // attach rolling count bolt using fields grouping - parallelism of 5
        // builder.setBolt("rolling-count-bolt", new RollingCountBolt(30, 10), 1).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));

        topology.setBolt("top-score-bolt", new TopNScoreBolt(TOP_N), 1).shuffleGrouping("filter-bolt");

        // attach the report bolt using global grouping - parallelism of 1
        topology.setBolt("dump-bolt", new DumpBolt(), 1).globalGrouping("top-score-bolt").globalGrouping("total-rankings-bolt").globalGrouping("tweet-spout");

        //topology.setBolt("tweet-summary-bolt", new SummaryTweetBolt(), 4).shuffleGrouping("tweet-spout");

//        builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");
//        builder.setBolt("infoBolt", new InfoBolt(), 10).fieldsGrouping("parse-tweet-bolt", new Fields("county_id"));
//        builder.setBolt("top-words", new TopWords(), 10).fieldsGrouping("infoBolt", new Fields("county_id"));
//        builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("top-words");
//
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
