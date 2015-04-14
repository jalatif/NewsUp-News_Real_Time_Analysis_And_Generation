package SentimentAnalysis;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class GetUserTimeline {


    public static void main(String[] args) {
        // gets Twitter instance with default credentials
        String consumerKey="U5qRMuLtVHY5Fvf5vr2r9pfRH";
        String consumerSecret="MDO4ZapZzzRPj3poFwarUE2wH8WhNlkDzuNfA34LOvtP4b18wT";
        String accessToken="144279822-l0Bb4Z2j3uKREd3RUttp4RFv2QcLR2ZN4hbocvlU";
        String accessSecret="frH1WZGTstlElZvqMA7rOd3Pwpf9cH7q02KSazvZUHgGO";

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(consumerKey);
        configurationBuilder.setOAuthConsumerSecret(consumerSecret);
        configurationBuilder.setOAuthAccessToken(accessToken);
        configurationBuilder.setOAuthAccessTokenSecret(accessSecret);

        Twitter twitter = new TwitterFactory(configurationBuilder.build()).getInstance();
        try {
            String user = "jalatifabhi";
            int page_num = 1;
            Paging paging = new Paging(page_num, 10);
            List<Status> statuses = Collections.EMPTY_LIST;
            System.out.println("Showing @" + user + "'s user timeline.");
            long sinceId = -1, prev_since_id = -1;

            while (statuses.isEmpty()) {
                statuses = twitter.getUserTimeline(user, paging);
                while (!statuses.isEmpty()) {
                    long maxId = -1;
                    boolean endNext = false;
                    for (Status status : statuses) {
                        if (status.getId() < prev_since_id) {
                            endNext = true;
                            break;
                        }
                        System.out.println("*******************************");
                        System.out.print("ID = " + status.getId());
                        System.out.println(" Text = " + status.getText() + " Date = " + status.getCreatedAt().toString());

                        System.out.println("*******************************\n");
                        maxId = Math.max(maxId, status.getId());
                        sinceId = Math.max(sinceId, status.getId());
                    }
                    if (endNext) break;
                    paging.setMaxId(maxId);
                    System.out.println("MaxId = " + paging.getMaxId());
                    paging.setPage(++page_num);

                    System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
                    System.out.println();
                    Thread.sleep(500);
                    statuses = twitter.getUserTimeline(user, paging);
                }
                page_num = 1;
                prev_since_id = sinceId;
                System.out.println(sinceId);
                if (sinceId != -1) {
                    paging = new Paging(sinceId);
                } else {
                    paging = new Paging(page_num, 10);
                }
                statuses = Collections.EMPTY_LIST;

                Thread.sleep(1500);
            }
        } catch (TwitterException e) {
            e.printStackTrace();
            System.out.println("Failed to get timeline: " + e.getMessage());
            System.exit(-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
