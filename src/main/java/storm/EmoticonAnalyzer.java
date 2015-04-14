package storm;

import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import Utils.Emoji;

/**
 * Created by manshu on 3/24/15.
 */
public class EmoticonAnalyzer {

    private static final String SPACE_EXCEPTIONS = "\\n\\r";
    public static final String SPACE_CHAR_CLASS = "\\p{C}\\p{Z}&&[^" + SPACE_EXCEPTIONS + "\\p{Cs}]";
    public static final String SPACE_REGEX = "[" + SPACE_CHAR_CLASS + "]";

    public static final String PUNCTUATION_CHAR_CLASS = "\\p{P}\\p{M}\\p{S}" + SPACE_EXCEPTIONS;
    public static final String PUNCTUATION_REGEX = "[" + PUNCTUATION_CHAR_CLASS + "]";

    private static final String EMOTICON_DELIMITER =
            SPACE_REGEX + "|" + PUNCTUATION_REGEX;

    public static final Pattern SMILEY_REGEX_PATTERN = Pattern.compile(":[)DdpP]|:[ -]\\)|<3", Pattern.CASE_INSENSITIVE);
    public static final Pattern FROWNY_REGEX_PATTERN = Pattern.compile(":[(<]|:[ -]\\(", Pattern.CASE_INSENSITIVE);
    public static final Pattern EMOTICON_REGEX_PATTERN =
            Pattern.compile("(?<=^|" + EMOTICON_DELIMITER + ")?("
                    + SMILEY_REGEX_PATTERN.pattern() + "|" + FROWNY_REGEX_PATTERN.pattern()
                    + ")+(?=$|" + EMOTICON_DELIMITER + ")", Pattern.CASE_INSENSITIVE);

    public static final Pattern EMOJI_REGEX = Pattern.compile("([\uD83C-\uDBFF\uDC00-\uDFFF])+", Pattern.CASE_INSENSITIVE);
    public static final Pattern EMOTICON_REGEX = Pattern.compile("[\uF301-\uF618]+", Pattern.CASE_INSENSITIVE);
    public static final Pattern EMOTICON_HEX_REGEX = Pattern.compile("0x1[F301-F618]+", Pattern.CASE_INSENSITIVE);

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    Set<String> ultra_unhappy_emoji = new HashSet<>(Arrays.asList("\ude3e", "\udc7f", "\ude21"));
    Set<String> extra_unhappy_emoji = new HashSet<>(Arrays.asList("\ude27", "\ude31", "\ude20", "\ude40", "\ude2d", "\ude1f", "\ude08"));
    Set<String> medium_unhappy_emoji = new HashSet<>(Arrays.asList("\ude29", "\ude2b", "\ude28", "\ude12", "\ude16", "\ude23", "\ude30", "\ude22", "\ude15", "\ude3f", "\ude33", "\ude2c", "\ude2e", "\ude1e"));
    Set<String> unhappy_emoji = new HashSet<>(Arrays.asList("\ude1c", "\ude13", "\ude35", "\ude25", "\ude37", "\ude14", "\ude01", "\ude26", "\ude2f"));
    Set<String> neutral_emoji = new HashSet<>(Arrays.asList("\ude34", "\ude36", "\ude11", "\ude1d", "\ude10", "\ude2a"));
    Set<String> happy_emoji = new HashSet<>(Arrays.asList("\ude0e", "\ude1b", "\ude06"));
    Set<String> medium_happy_emoji = new HashSet<>(Arrays.asList("\ude19", "\ude0a", "\ude0c", "\ude17", "\ude32", "\ude3a", "\ude05", "\ude04", "\ude0f", "\ude00", "\ude03", "\ude3d", "\ude38", "\ude1a", "263a", "fe0f", "\ude3c"));
    Set<String> extra_happy_emoji = new HashSet<>(Arrays.asList("\ude0d", "\ude18", "\ude39", "\ude0b", "\ude07", "\ude02", "\ude09", "\ude3b"));
    Set<String> ultra_happy_emoji = new HashSet<>(Arrays.asList("\ude24"));

    Set<String> happy = new HashSet<String>(Arrays.asList("1f601", "1f602", "1f603", "1f604", "1f605", "1f606", "1f609", "1f60A", "1f60B", "1f60D", "1f618", "1f61A", "1f61C", "1f61D", "1f624", "1f632", "1f638", "1f639", "1f63A", "1f63B", "1f63D", "1f647", "1f64B", "1f64C", "1f64F", "U+270C", "U+2728", "U+2764", "U+263A", "U+2665", "U+3297", "1f31F", "1f44F", "1f48B", "1f48F", "1f491", "1f492", "1f493", "1f495", "1f496", "1f497", "1f498", "1f499", "1f49A", "1f49B", "1f49C", "1f49D", "1f49D", "1f49F", "1f4AA", "1f600", "1f607", "1f608", "1f60E", "1f617", "1f619", "1f61B", "1f31E"));
    Set<String> mediumHappy = new HashSet<String>(Arrays.asList("1f60C", "1f60F", "1f633", "1f63C", "1f646", "U+2B50", "1f44D", "1f44C"));
    Set<String> neutral = new HashSet<String>(Arrays.asList("1f614", "1f623", "U+2753", "U+2754", "1f610", "1f611", "1f62E", "1f636"));
    Set<String> mediumUnhappy = new HashSet<String>(Arrays.asList("1f612", "1f613", "1f616", "1f61E", "1f625", "1f628", "1f62A", "1f62B", "1f637", "1f635", "1f63E", "U+26A0", "1f44E", "1f4A4", "1f615", "1f61F", "1f62F", "1f634"));
    Set<String> unhappy = new HashSet<String>(Arrays.asList("1f620", "1f621", "1f622", "1f629", "1f62D", "1f630", "1f631", "1f63F", "1f640", "1f645", "1f64D", "1f64E", "U+274C", "U+274E", "1f494", "1f626", "1f627", "1f62C"));

    Set<String> happy_emoticon = new HashSet<String>(Arrays.asList("\uf601", "\uf602", "\uf603", "\uf604", "\uf605", "\uf606", "\uf609", "\uf60A", "\uf60B", "\uf60D", "\uf618", "\uf61A", "\uf61C", "\uf61D", "\uf624", "\uf632", "\uf638", "\uf639", "\uf63A", "\uf63B", "\uf63D", "\uf647", "\uf64B", "\uf64C", "\uf64F", "U+270C", "U+2728", "U+2764", "U+263A", "U+2665", "U+3297", "\uf31F", "\uf44F", "\uf48B", "\uf48F", "\uf491", "\uf492", "\uf493", "\uf495", "\uf496", "\uf497", "\uf498", "\uf499", "\uf49A", "\uf49B", "\uf49C", "\uf49D", "\uf49D", "\uf49F", "\uf4AA", "\uf600", "\uf607", "\uf608", "\uf60E", "\uf617", "\uf619", "\uf61B", "\uf31E"));
    Set<String> mediumHappy_emoticon = new HashSet<String>(Arrays.asList("\uf60C", "\uf60F", "\uf633", "\uf63C", "\uf646", "U+2B50", "\uf44D", "\uf44C"));
    Set<String> neutral_emoticon = new HashSet<String>(Arrays.asList("\uf614", "\uf623", "U+2753", "U+2754", "\uf610", "\uf611", "\uf62E", "\uf636"));
    Set<String> mediumUnhappy_emoticon = new HashSet<String>(Arrays.asList("\uf612", "\uf613", "\uf616", "\uf61E", "\uf625", "\uf628", "\uf62A", "\uf62B", "\uf637", "\uf635", "\uf63E", "U+26A0", "\uf44E", "\uf4A4", "\uf615", "\uf61F", "\uf62F", "\uf634"));
    Set<String> unhappy_emoticon = new HashSet<String>(Arrays.asList("\uf620", "\uf621", "\uf622", "\uf629", "\uf62D", "\uf630", "\uf631", "\uf63F", "\uf640", "\uf645", "\uf64D", "\uf64E", "U+274C", "U+274E", "\uf494", "\uf626", "\uf627", "\uf62C"));

    public int getEmoticonScore(String s) {
        Matcher matcher;
        int score = 0;

        matcher = EMOTICON_REGEX_PATTERN.matcher(s);
        while (matcher.find()) {
            String match = matcher.group();
            if (SMILEY_REGEX_PATTERN.matcher(match).matches())
                score += 1;
            else if (FROWNY_REGEX_PATTERN.matcher(match).matches())
                score -= 1;
            System.out.println(match + " " + score);
        }
        System.out.println("Score = " + score);

        matcher = EMOJI_REGEX.matcher(s);
        while (matcher.find()) {
            String match = matcher.group().toLowerCase();
            //String match = StringEscapeUtils.unescapeJava(match1);
            //byte[] bytes = match1.getByteString match = new String(bytes, UTF_8);
            boolean bm5 = ultra_unhappy_emoji.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bm4 = extra_unhappy_emoji.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bm2 = medium_unhappy_emoji.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bm1 = unhappy_emoji.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bn0 = neutral_emoji.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bp1 = happy_emoji.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bp2 = medium_happy_emoji.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bp4 = extra_happy_emoji.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bp5 = ultra_happy_emoji.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();

            if (bm5) {score += -5;}
            else if (bm4) {score += -4;}
            else if (bm2) {score += -2;}
            else if (bm1) {score += -1;}
            else if (bn0)  {score += 0;}
            else if (bp1) {score += 1;}
            else if (bp2) {score += 3;}
            else if (bp4) {score += 4;}
            else if (bp5) {score += 5;}
            System.out.println(match + " " + score);
        }
        System.out.println("Score = " + score);

        matcher = EMOTICON_REGEX.matcher(s);
        while (matcher.find()) {
            String match = matcher.group().toLowerCase();

            boolean bp2 = happy_emoticon.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bp1 = mediumHappy_emoticon.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bn0 = neutral_emoticon.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bm1 = mediumUnhappy_emoticon.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();
            boolean bm2 = unhappy_emoticon.stream().filter(str -> match.contains(str.toLowerCase())).findAny().isPresent();

            if (bp2) {score += 2;}
            else if (bp1) {score += 1;}
            else if (bn0) {score += 0;}
            else if (bm1) {score += -1;}
            else if (bm2) {score += -2;}

            System.out.println(match + " " + score);
        }
        System.out.println("Score = " + score);

        matcher = EMOTICON_REGEX.matcher(s);
        while (matcher.find()) {
            String match = matcher.group().toLowerCase();
            char[] ca = match.toCharArray();
            for(int j = 0; j < ca.length; j=j+2  ) {
                System.out.println( String.format("%04x", Character.toCodePoint(ca[j], ca[j+1])) );
                String unicodeString = String.format("%04x", Character.toCodePoint(ca[j], ca[j+1]));
                if(happy.contains( unicodeString )){
                    score += 2;
                } else if(mediumHappy.contains( unicodeString )){
                    score += 1;
                } else if(neutral.contains( unicodeString )){
                    score += 0;
                } else if(mediumUnhappy.contains( unicodeString )){
                    score += -1;
                } else if(unhappy.contains( unicodeString )){
                    score += -2;
                }
            }
        }

        return score;
    }

    public static void main(String[] args) {
        EmoticonAnalyzer eme = new EmoticonAnalyzer();
        String s = "Today's Reality:\n" +
                "\n" +
                "Big House\uD83C\uDFE4\n" +
                "Small Family\uD83D\uDe24\n" +
                "\n" +
                "\n" +
                "More Degrees\uD83D\uDCD1\uD83D\uDD16\n" +
                "Less Common Sense\uD83D\uDCAC\uD83D\uDCAD\n" +
                "\n" +
                "\n" +
                "Advanced Medicine\n" +
                "Poor Health\n" +
                "\n" +
                "\n" +
                "Touched Moon \uD83C\uDF0D\uD83C\uDF1C\n" +
                "Neighbours Unknown\n" +
                "\n" +
                "\n" +
                "High Income\uD83D\uDCB3\uD83D\uDCB8\uD83D\uDCB6\n" +
                "Less peace of Mind\uD83D\uDE47\n" +
                "\n" +
                "\n" +
                "High IQ\uD83D\uDCDA\uD83D\uDCF0\n" +
                "Less Emotions\uD83D\uDD2A\n" +
                "\n" +
                "\n" +
                "Good Knowledge\uD83D\uDCD5\uD83D\uDCD9\uD83D\uDCD7\uD83D\uDCD3\n" +
                "Less Wisdom\uD83D\uDE45\n" +
                "\n" +
                "\n" +
                "Number of affairs\uD83D\uDEB6\n" +
                "No true love\n" +
                "\n" +
                "\n" +
                "Lot of friends on Facebook\n" +
                "No best friends\uD83D\uDC6C\uD83D\uDC6D\n" +
                "\n" +
                "\n" +
                "More alcohol\uD83C\uDF77\n" +
                "Less water\n" +
                "\n" +
                "\n" +
                "Lots of Human\n" +
                "Less Humanity\uD83D\uDC7A\uD83D\uDE3E\n" +
                "\n" +
                "\n" +
                "Costly Watches???\n" +
                "But No time \uD83D\uDD53\uD83D\uDD54\uD83D\uDD57\n" +
                "\n" +
                "Ghor Kalyug\n" +
                "\n" +
                "\"LIFE ENJOY KARO YAAR\"\n" +
                "\n" +
                "Apna Khayal Karo,\n" +
                "Hamesha SMILE karO:))\n" +
                "\n\ud83d\ude30" +
                "Daily:\n\ud83d\uDe05" +
                "1 Apple= No Doctor\n" +
                "1Tulsi Patta= No Cancer\n" +
                "1 Nimbu\uD83C\uDF4B= No Fat\n" +
                "1 Glass Milk= No Bone Problms\n" +
                "3 Ltr Water= Skin Saaf\n" +
                "&\n" +
                "Daily Whatsapp = No stress :(, Mood fresh \uD83C\uDF77\uD83C\uDF79\n" +
                "Ye msg aap sirf 3 logo ko bej sakte hai uske baad send nahi hoga\n" +
                "Try it\n" +
                "I am also shocked\n" +
                "Chat conversation end\n \uf601";

        String str = "Life is good :) I am enjoying it :) Life is good \uD83D\uDE0A I am enjoying it \uD83D\uDE0A gth: %i\", @\"\uD83D\uDC68\".leng";

        eme.getEmoticonScore(str);
        System.out.println(Emoji.replaceInText(str));
    }
}
