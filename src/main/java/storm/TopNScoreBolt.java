package storm;

import Utils.TweetContent;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;

import java.util.*;

/**
 * Created by manshu on 3/31/15.
 */
public class TopNScoreBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private PriorityQueue<TweetContent> priorityQueue;
    private int TopN = 10;
    private StringBuffer top_tweets;
    private long prev_time_millis;
    private int emitFreqSeconds;
    private final int DEFAULT_EMIT_FREQUENCY = 5;
    protected final static String DELIMITER = "::";

    public TopNScoreBolt(int topN, int emitFreqSeconds) {
        this.TopN = topN;
        this.emitFreqSeconds = emitFreqSeconds;
        this.prev_time_millis = System.currentTimeMillis();
        top_tweets = new StringBuffer();
    }

    public TopNScoreBolt(int topN) {
        this.TopN = topN;
        this.emitFreqSeconds = DEFAULT_EMIT_FREQUENCY;
        this.prev_time_millis = System.currentTimeMillis();
        top_tweets = new StringBuffer();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.priorityQueue = new PriorityQueue<TweetContent>(new Comparator() {
            @Override
            public int compare(Object o, Object t1) {
                TweetContent tweetContent1 = (TweetContent) o;
                TweetContent tweetContent2 = (TweetContent) t1;
                return tweetContent1.getScore().compareTo(tweetContent2.getScore());
            }
        });
        this.prev_time_millis = System.currentTimeMillis();
        top_tweets = new StringBuffer();
    }

    @Override
    public void execute(Tuple tuple) {
        String tweet_info = tuple.getStringByField("tweet-content");
        TweetContent tweetContent = new TweetContent(tweet_info);
        while (priorityQueue.size() > this.TopN) {
            priorityQueue.poll();
        }

        if (priorityQueue.size() == this.TopN) {
            TweetContent tweetContent1 = priorityQueue.peek();
            if (tweetContent.getScore() > tweetContent1.getScore()) {
                priorityQueue.remove(tweetContent1);
                priorityQueue.add(tweetContent);
            }
        } else if (priorityQueue.size() < this.TopN) {
            priorityQueue.add(tweetContent);
        }
        long current_time_millis = System.currentTimeMillis();
        if ( (current_time_millis - prev_time_millis) >= (emitFreqSeconds * 1000) ) {
            top_tweets.setLength(0);
            Iterator<TweetContent> iterator = priorityQueue.iterator();
            while (iterator.hasNext()) {
                TweetContent temp = iterator.next();
                top_tweets.append(temp.toString()).append(DELIMITER);
                System.out.println("RVatif = " + temp.toString());
            }
            outputCollector.emit(new Values(top_tweets.toString()));
            prev_time_millis = System.currentTimeMillis();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("top-tweets"));
    }
}
