package SentimentAnalysis;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.trees.*;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;
import java.util.Properties;

/**
 * Created by manshu on 3/23/15.
 */
public class TryNLP {

    static StanfordCoreNLP pipeline;

    public static void init() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        pipeline = new StanfordCoreNLP(props);
    }

    public static int findSentiment(String tweet) {

        int mainSentiment = 0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(tweet);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }

    private static void getTwitterTimeline() throws TwitterException {
        String consumerKey="U5qRMuLtVHY5Fvf5vr2r9pfRH";
        String consumerSecret="MDO4ZapZzzRPj3poFwarUE2wH8WhNlkDzuNfA34LOvtP4b18wT";
        String accessToken="144279822-l0Bb4Z2j3uKREd3RUttp4RFv2QcLR2ZN4hbocvlU";
        String accessSecret="frH1WZGTstlElZvqMA7rOd3Pwpf9cH7q02KSazvZUHgGO";

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessSecret);

        TwitterFactory factory = new TwitterFactory(cb.build());
        Twitter twitter = factory.getInstance();
        String[] srch = new String[] {"BBCWorld"};
        ResponseList<User> users = twitter.lookupUsers(srch);
        for (User user : users) {
            System.out.println("Friend's Name " + user.getName()); // this print my friends name
            if (user.getStatus() != null)
            {
                System.out.println("Friend timeline");
                List<Status> statusess = twitter.getUserTimeline(user.getName());
                for (Status status3 : statusess)
                {
                    System.out.println(status3.getText());
                }

            }
        }
    }

    public static void main(String[] args) throws TwitterException {
//        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
//        Properties props = new Properties();
//        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
//        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
//
//        // read some text in the text variable
//        String text = "asdasd"; // Add your text here!
//
//        // create an empty Annotation just with the given text
//        Annotation document = new Annotation(text);
//
//        // run all Annotators on this text
//        pipeline.annotate(document);
//
//        // these are all the sentences in this document
//        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
//        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
//
//        for(CoreMap sentence: sentences) {
//            // traversing the words in the current sentence
//            // a CoreLabel is a CoreMap with additional token-specific methods
//            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
//                // this is the text of the token
//                String word = token.get(CoreAnnotations.TextAnnotation.class);
//                // this is the POS tag of the token
//                String pos = token.get(PartOfSpeechAnnotation.class);
//                // this is the NER label of the token
//                String ne = token.get(NamedEntityTagAnnotation.class);
//            }
//
//            // this is the parse tree of the current sentence
//            Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
//
//            // this is the Stanford dependency graph of the current sentence
//            SemanticGraph dependencies = sentence.get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
//        }
//
//        // This is the coreference link graph
//        // Each chain stores a set of mentions that link to each other,
//        // along with a method for getting the most representative mention
//        // Both sentence and token offsets start at 1!
//        Map<Integer, CorefChain> graph =
//                document.get(CorefCoreAnnotations.CorefChainAnnotation.class);

        String a = "@BBCWorld I have no idea talk to the guys in the financials @CGasparino he would know it @jimcramer \\uD83D\\uDC95";
        MaxentTagger tagger =  new MaxentTagger("/home/manshu/Setups/StanfordNLP/stanford-postagger-full-2015-01-30/models/english-left3words-distsim.tagger");
        String tagged = tagger.tagString(a);
        System.out.println(tagged);
//
//        init();
//        System.out.println(findSentiment("@BBCWorld I have no idea talk to the guys in the financials @CGasparino he would know it @jimcramer \uD83D\uDC95"));
//        getTwitterTimeline();
    }
}
