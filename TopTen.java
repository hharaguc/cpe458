// Holly Haraguchi
// CPE 458, Winter 2017
// Lab 11, Part 1
// Outputs the top 10 most common words in "Pride and Prejudice" by Jane Austen, along with their frequency

// Section 1: Imports
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.List;

// Exception handling
import java.io.IOException;

public class TopTen {

    // Mapper Class
    public static class TokenMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Keeping it simple--only using whitespaces as delimiters
            String[] words = value.toString().split("\\s+|\\t+");
            List<String> stopWords = Arrays.asList(
              "a",
              "about",
              "above",
              "across",
              "after",
              "afterwards",
              "again",
              "against",
              "all",
              "almost",
              "alone",
              "along",
              "already",
              "also",
              "although",
              "always",
              "am",
              "among",
              "amongst",
              "amoungst",
              "amount",
              "an",
              "and",
              "another",
              "any",
              "anyhow",
              "anyone",
              "anything",
              "anyway",
              "anywhere",
              "are",
              "around",
              "as",
              "at",
              "back",
              "be",
              "became",
              "because",
              "become",
              "becomes",
              "becoming",
              "been",
              "before",
              "beforehand",
              "behind",
              "being",
              "below",
              "beside",
              "besides",
              "between",
              "beyond",
              "bill",
              "both",
              "bottom",
              "but",
              "by",
              "call",
              "can",
              "cannot",
              "cant",
              "co",
              "computer",
              "con",
              "could",
              "couldnt",
              "cry",
              "de",
              "describe",
              "detail",
              "do",
              "done",
              "down",
              "due",
              "during",
              "each",
              "eg",
              "eight",
              "either",
              "eleven",
              "else",
              "elsewhere",
              "empty",
              "enough",
              "etc",
              "even",
              "ever",
              "every",
              "everyone",
              "everything",
              "everywhere",
              "except",
              "few",
              "fifteen",
              "fify",
              "fill",
              "find",
              "fire",
              "first",
              "five",
              "for",
              "former",
              "formerly",
              "forty",
              "found",
              "four",
              "from",
              "front",
              "full",
              "further",
              "get",
              "give",
              "go",
              "had",
              "has",
              "hasnt",
              "have",
              "he",
              "hence",
              "her",
              "here",
              "hereafter",
              "hereby",
              "herein",
              "hereupon",
              "hers",
              "herse",
              "him",
              "himse",
              "his",
              "how",
              "however",
              "hundred",
              "i",
              "ie",
              "if",
              "in",
              "inc",
              "indeed",
              "interest",
              "into",
              "is",
              "it",
              "its",
              "itse",
              "keep",
              "last",
              "latter",
              "latterly",
              "least",
              "less",
              "ltd",
              "made",
              "many",
              "may",
              "me",
              "meanwhile",
              "might",
              "mill",
              "mine",
              "more",
              "moreover",
              "most",
              "mostly",
              "move",
              "much",
              "must",
              "my",
              "myse",
              "name",
              "namely",
              "neither",
              "never",
              "nevertheless",
              "next",
              "nine",
              "no",
              "nobody",
              "none",
              "noone",
              "nor",
              "not",
              "nothing",
              "now",
              "nowhere",
              "of",
              "off",
              "often",
              "on",
              "once",
              "one",
              "only",
              "onto",
              "or",
              "other",
              "others",
              "otherwise",
              "our",
              "ours",
              "ourselves",
              "out",
              "over",
              "own",
              "part",
              "per",
              "perhaps",
              "please",
              "put",
              "rather",
              "re",
              "same",
              "see",
              "seem",
              "seemed",
              "seeming",
              "seems",
              "serious",
              "several",
              "she",
              "should",
              "show",
              "side",
              "since",
              "sincere",
              "six",
              "sixty",
              "so",
              "some",
              "somehow",
              "someone",
              "something",
              "sometime",
              "sometimes",
              "somewhere",
              "still",
              "such",
              "system",
              "take",
              "ten",
              "than",
              "that",
              "the",
              "their",
              "them",
              "themselves",
              "then",
              "thence",
              "there",
              "thereafter",
              "thereby",
              "therefore",
              "therein",
              "thereupon",
              "these",
              "they",
              "thick",
              "thin",
              "third",
              "this",
              "those",
              "though",
              "three",
              "through",
              "throughout",
              "thru",
              "thus",
              "to",
              "together",
              "too",
              "top",
              "toward",
              "towards",
              "twelve",
              "twenty",
              "two",
              "un",
              "under",
              "until",
              "up",
              "upon",
              "us",
              "very",
              "via",
              "was",
              "we",
              "well",
              "were",
              "what",
              "whatever",
              "when",
              "whence",
              "whenever",
              "where",
              "whereafter",
              "whereas",
              "whereby",
              "wherein",
              "whereupon",
              "wherever",
              "whether",
              "which",
              "while",
              "whither",
              "who",
              "whoever",
              "whole",
              "whom",
              "whose",
              "why",
              "will",
              "with",
              "within",
              "without",
              "would",
              "yet",
              "you",
              "your",
              "yours",
              "yourself",
              "yourselves"
           );
            // Output each word with an arbitrary, common key
            for (String word : words) {
                String noCase = word.toLowerCase().trim();

                // Double check for whitespace; make sure the word is not a stop word
                if (!stopWords.contains(noCase) && noCase.length() > 0) {
                    context.write(new Text("key"), new Text(word));
                }
            }
        }
    }

    // Reducer Class
    // Single reducer design
    public static class TokenReducer extends Reducer< Text, Text, Text, Text > {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Keep track of words and their respective counts
            HashMap<String, Integer> wordCounts = new HashMap<String, Integer>();

            // Count the words and add them to the HashMap
            for (Text val : values) {
                String curWord = val.toString();
                
                if (wordCounts.containsKey(curWord)) {
                    wordCounts.put(curWord, wordCounts.get(curWord) + 1);
                }
                else {
                    wordCounts.put(curWord, 1);
                } 
            }

            Map<String, Integer> topTen = new LinkedHashMap<>();
            
            // Sort the map: http://www.mkyong.com/java8/java-8-how-to-sort-a-map/
            wordCounts.entrySet().stream()
                      .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                      .limit(10)
                      .forEachOrdered(x -> topTen.put(x.getKey(), x.getValue()));
           for (Map.Entry<String, Integer> entry : topTen.entrySet()) {
               String newKey = entry.getKey();
               Integer value = entry.getValue();
               context.write(new Text(newKey), new Text(Integer.toString(value)));
         }  
       } 
    }

    // Section 4:  MapReduce Driver
    public static void main(String[] args) throws Exception {
        // Step 1: get a new MapReduce Job object
        Job job = Job.getInstance();  

        // Step 2: register the MapReduce class
        job.setJarByClass(TopTen.class);  

        // Step 3:  Set Input and Output files
        FileInputFormat.setInputPaths(job, new Path("pride.txt")); 
        FileOutputFormat.setOutputPath(job, new Path("topTen-out")); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job.setMapperClass(TokenMapper.class);
        job.setReducerClass(TokenReducer.class);
        
        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Top 10 Words - Holly Haraguchi");

        // Step 7
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}

