package storm;

import Utils.CountiesLookup;
import Utils.TweetContent;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

import java.util.*;

/**
 * Created by manshu on 3/24/15.
 */
public class ParseTweetBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private String delims = "[ .,?!]+";
    private MaxentTagger tagger;
    private CountiesLookup countiesLookup;
    private StringBuffer output;

    private String[] skip_words = {"rt", "to", "me","la","on","that","que",
            "followers","watch","know","not","have","like","I'm","new","good","do",
            "more","es","te","followers","Followers","las","you","and","de","my","is",
            "en","una","in","for","this","go","en","all","no","don't","up","are",
            "http","http:","https","https:","http://","https://","with","just","your",
            "para","want","your","you're","really","video","it's","when","they","their","much",
            "would","what","them","todo","FOLLOW","retweet","RETWEET","even","right","like",
            "bien","Like","will","Will","pero","Pero","can't","were","Can't","Were","TWITTER",
            "make","take","This","from","about","como","esta","follows","followed"};
    private Set<String> skip_words_set;

    private String[] partOfSpeechTags(String text) {
        String tagged_string = tagger.tagString(text);
        String[] tag_words = tagged_string.split(delims);
        return tag_words;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        tagger = new MaxentTagger("english-left3words-distsim.tagger");
        skip_words_set = new HashSet<>(Arrays.asList(skip_words));
        countiesLookup = new CountiesLookup();
        output = new StringBuffer();
    }

    private boolean isHashTag(String word) {
        if (word.startsWith("#"))
            return true;
        return false;
    }

    @Override
    public void execute(Tuple input) {
        String tweet_info = input.getStringByField("tweet-content");
        TweetContent tweetContent = new TweetContent(tweet_info);
//        double latitude = tweetContent.getLatitude();
//        double longitude = tweetContent.getLongitude();
//        String county_id = countiesLookup.getCountyCodeByGeo(latitude, longitude);
//
//        //int sentiment_value = input.getIntegerByField("sentiment");
//        String[] tag_words = partOfSpeechTags(tweetContent.getTweet());
        String[] tokens = tweetContent.getTweet().split(delims);
        //output.setLength(0);

        for (String word : tokens) {
//            System.out.println(word);
            if (skip_words_set.contains(word)) continue;
            if (!isHashTag(word)) continue;
            outputCollector.emit(new Values(tweetContent.getId(), word));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-id", "tweet-word"));
    }

}
