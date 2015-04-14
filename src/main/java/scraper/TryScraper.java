package scraper;

import com.jaunt.UserAgent;
import com.jaunt.JauntException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.List;

/**
 * Created by manshu on 3/22/15.
 */
public class TryScraper {

    public static void main(String[] args) throws JauntException {

        String[] websites = {"BBCWorld", "timesofindia"};

        UserAgent userAgent = new UserAgent(); // headless browser
        System.out.println("SETTINGS:\n" + userAgent.settings);
        userAgent.settings.autoSaveAsHTML = true;
//        userAgent.visit("https://twitter.com/" + websites[0]);
//
//        Elements tweets = userAgent.doc.findEvery("<div class=ProfileTweet-contents>").findEvery("<div class=ProfileTweet-authorDetails>");
//        for(Element tweet : tweets) {
//            System.out.println(tweet);
//        }

        //userAgent.visit("https://twitter.com/BBCWorld/status/579915554187710465");
        userAgent.visit("https://twitter.com/bbcweather/status/580455369362800640");
        Document doc = Jsoup.parse(userAgent.doc.innerHTML());

        //System.out.println(doc.text());

        Elements tweets = doc.select("li.js-simple-tweet").select("div.content").select("p");
        //Elements tweets = userAgent.doc.findEvery("<li class=js-simple-tweet>").findEach("<div class=content>").findEach("<p>");
        int i = 1;
        for(Element tweet : tweets) {
            System.out.println("Tweet " + i++ + " = " + tweet.text());
        }
    }
}
