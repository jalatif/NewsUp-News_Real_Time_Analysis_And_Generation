package Utils;

import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.User;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by manshu on 3/23/15.
 */
public class TweetContent {

    public static final String DELIMITER = "#@";
    public static final String IDELIMITER = ";;";

    private Long id;
    private String tweet;
    private String user;
    private int retweets;
    private int favorites;
    private List<String> urls;
    private String time;
    private double latitude, longitude;
    private Integer score;

    public TweetContent(Status status) {
        id = status.getId();
        tweet = status.getText();
        user = status.getUser().getScreenName();
        retweets = status.getRetweetCount();
        favorites = status.getFavoriteCount();
        urls = new ArrayList<String>();
        if (status.getURLEntities().length > 0) {
            for (URLEntity urlEntity : status.getURLEntities())
                if (urlEntity.getExpandedURL().startsWith("http"))
                    urls.add(urlEntity.getExpandedURL());
        }
        time = status.getCreatedAt().toString();
        if (status.getGeoLocation() != null) {
            latitude = status.getGeoLocation().getLatitude();
            longitude = status.getGeoLocation().getLongitude();
        } else {
            latitude = -1.0;
            longitude = -1.0;
        }
        score = 0;
    }

    public TweetContent(String tweet_content) {
        String[] tweet_info = tweet_content.split(DELIMITER);
        id = Long.valueOf(tweet_info[0]);
        tweet = tweet_info[1];
        user = tweet_info[2];
        retweets = Integer.parseInt(tweet_info[3]);
        favorites = Integer.parseInt(tweet_info[4]);
        String[] tweet_urls = tweet_info[5].split(IDELIMITER);
        urls = new ArrayList<String>();
        if (tweet_urls.length > 0) {
            for (String url : tweet_urls)
                if (url.startsWith("http"))
                    urls.add(url);
        }
        time = tweet_info[6];

        latitude = Double.parseDouble(tweet_info[7]);
        longitude = Double.parseDouble(tweet_info[8]);

        score = Integer.parseInt(tweet_info[9]);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(id).append(DELIMITER);
        stringBuilder.append(tweet).append(DELIMITER);
        stringBuilder.append(user).append(DELIMITER);
        stringBuilder.append(retweets).append(DELIMITER);
        stringBuilder.append(favorites).append(DELIMITER);
        if (urls.size() > 0) {
            for (int i = 0; i < urls.size() - 1; i++)
                stringBuilder.append(urls.get(i)).append(IDELIMITER);
            stringBuilder.append(urls.get(urls.size() - 1));
        }
        stringBuilder.append(DELIMITER);
        stringBuilder.append(time).append(DELIMITER);
        stringBuilder.append(latitude).append(DELIMITER);
        stringBuilder.append(longitude).append(DELIMITER);
        stringBuilder.append(score);
        return stringBuilder.toString();
    }

    public String getTweetStatusUrl() {
        return "http://www.twitter.com/" + user + "/status/" + String.valueOf(id);
    }

    public Long getId() {
        return id;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTweet() {
        return tweet;
    }

    public void setTweet(String tweet) {
        this.tweet = tweet;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public int getRetweets() {
        return retweets;
    }

    public void setRetweets(int retweets) {
        this.retweets = retweets;
    }

    public int getFavorites() {
        return favorites;
    }

    public void setFavorites(int favorites) {
        this.favorites = favorites;
    }

    public List<String> getUrls() {
        return urls;
    }

    public void setUrls(List<String> urls) {
        this.urls = urls;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
}
