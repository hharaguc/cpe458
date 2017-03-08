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

// Exception handling
import java.io.IOException;

// Finds the top 10 most common words in "Pride and Prejudice" by Jane Austen
public class TopTen {

    // Mapper Class
    public static class TokenMapper extends Mapper< LongWritable, Text, Text, LongWritable > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Keeping it simple--only using a space as the delimeter
            String[] words = value.toString().split(" ");

            // Output each word with an arbitrary, common key
            for (String word : words) {
                context.write("key", word);
            }
        }
    }

    // Reducer Class
    // Single reducer design
    public static class TokenReducer extends Reducer< Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Keep track of words and their respective counts
            HashMap<String, Integer> wordCounts = new HashMap<String, Integer>();

            // Count the words and add them to the HashMap
            for (Text val : values) {
                String curWord = val.toString();

                if (wordCounts.contains(curWord)) {
                    wordCounts.put(curWord, wordCounts.get(curWord) + 1);
                }
                else {
                    wordCounts.put(curWord, 1);
                }
            }

            // Sort the map: http://www.mkyong.com/java8/java-8-how-to-sort-a-map/
            wordCounts.entrySet().stream()
                      .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                      .limit(10)
                      .forEachOrdered(x -> topTen.put(x.getKey(), x.getValue()));
            topTen.forEach((k,v) -> context.write(new Text(k), new Text(Integer.toString(v))));
        }
    }

    // Section 4:  MapReduce Driver
    public static void main(String[] args) throws Exception {
        // Step 1: get a new MapReduce Job object
        Job job = Job.getInstance();  

        // Step 2: register the MapReduce class
        job.setJarByClass(TopTen.class);  

        // Step 3:  Set Input and Output files
        FileInputFormat.setInputPaths(job, new Path("sense.txt")); 
        FileOutputFormat.setOutputPath(job, new Path("topTen-out")); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job.setMapperClass(TokenMapper.class);
        job.setReducerClass(TokenReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(LongWritable.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Top 10 Words - Holly Haraguchi");

        // Step 7
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}

