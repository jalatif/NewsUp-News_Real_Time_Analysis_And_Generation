package storm;

import Utils.TweetContent;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created by manshu on 3/24/15.
 */
public class SummaryTweetBolt extends BaseRichBolt {
    OutputCollector outputCollector;

    private String getSummary(String url) throws IOException {
        Process p = Runtime.getRuntime().exec(new String[]{"bash", "-c", "sumy lex-rank --length=10 --url=" + url});
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

        StringBuffer stringBuffer = new StringBuffer();
        boolean error = false;
        String s;
        System.out.println("Standard output of the command:\n");
        while ((s = stdInput.readLine()) != null) {
            System.out.println(s);
            stringBuffer.append(s);
        }

        // read any errors from the attempted command
        System.out.println("Standard error of the command (if any):\n");
        while ((s = stdError.readLine()) != null) {
            System.out.println(s);
            error = true;
        }

        if (!error) return stringBuffer.toString();
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String tweet_info = input.getStringByField("tweet-content");
        TweetContent tweetContent = new TweetContent(tweet_info);
        StringBuffer stringBuffer = new StringBuffer("");

        if (!tweetContent.getUrls().isEmpty()) {
            for (String url : tweetContent.getUrls()) {
                try {
                    System.out.println("JURL = " + url);
                    String s = getSummary(url);
                    if (s == null) continue;
                    stringBuffer.append(s).append("\n");
                } catch (IOException ie) {
                    System.out.println("Url can't be summarized: " + url);
                }
            }
            System.out.println("Tweet-url = " + tweetContent.getTweetStatusUrl() + "Summary = " + stringBuffer.toString());
            outputCollector.emit(new Values(tweetContent.getId(), stringBuffer.toString()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-id", "tweet-summary"));
    }
}
