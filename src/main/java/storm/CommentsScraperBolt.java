package storm;

import Utils.TweetContent;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.jaunt.ResponseException;
import com.jaunt.UserAgent;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by manshu on 3/24/15.
 */
public class CommentsScraperBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private UserAgent userAgent;

    private List<String> getComments(String url) {
        List<String> comments = null;
        try {
            userAgent.visit(url);
            Document doc = Jsoup.parse(userAgent.doc.innerHTML());
            Elements tweets = doc.select("li.js-simple-tweet").select("div.content").select("p");
            if (tweets == null) return comments;
            comments = new ArrayList<>();
            for(Element tweet : tweets) {
                comments.add(tweet.text());
            }
        } catch (ResponseException e) {
            e.printStackTrace();
        }
        return comments;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        userAgent = new UserAgent();
        System.out.println("SETTINGS:\n" + userAgent.settings);
        userAgent.settings.autoSaveAsHTML = true;
    }

    @Override
    public void execute(Tuple input) {
        String tweet_info = input.getStringByField("tweet-content");
        TweetContent tweetContent = new TweetContent(tweet_info);
        String tweet_url = tweetContent.getTweetStatusUrl();
        List<String> comments = getComments(tweet_url);
        if (comments != null) {
            int i = 0;
            for (String comment : comments) {
                outputCollector.emit(new Values(tweetContent.getId(), comment, i++));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-id", "comment", "comment-id"));
    }
}
