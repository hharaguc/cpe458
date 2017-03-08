// Holly Haraguchi
// CPE 458, Winter 2017
// Lab 11, Part 2
// Outputs the longest sentence in "Pride and Prejudice" by Jane Austen

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

// Finds the longest sentence in "Pride and Prejudice" by Jane Austen
public class MaxLineLen {

    // Mapper Class
    public static class MaxLenMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Regex found here: http://stackoverflow.com/questions/28554260/split-text-with-quotes-into-sentences-using-breakiterator-java
            String[] sentences = value.toString.split("(?<!Mrs?\\.)(?<=\\.)\\s+(?=(?:\"[^\"]*\"|[^\"])*$)");
            int maxLen = 0;
            String maxSent = "";

            // Find the longest sentence in this mapper
            for (String sentence : sentences) {
                // Don't include spaces in the sentence length
                int curLen = sentence.replace(" ", "").length()

                // New longest sentence found; update variables
                if (curLen > maxLen) {
                    maxLen = curLen;
                    maxSent = sentence;
                }
            }

            // Output the max sentence found with an arbitrary key
            context.write("key", new Text(maxSent + "," + maxLen));   
        }
    }

    // Reducer Class
    public static class MaxLenReducer extends Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxLen = 0;
            String maxSent = "";

            for (Text val : values) {
                String[] splits = val.toString().split(",");

                if (splits[1] > maxLen) {
                    maxLen = splits[1];
                    maxSent = splits[0];
                }
            }
            
            // Output the file's longest line + line length
            context.write(maxSent, maxLen);
        }
    }

    // Section 4:  MapReduce Driver
    public static void main(String[] args) throws Exception {
        // Step 1: get a new MapReduce Job object
        Job job = Job.getInstance();  

        // Step 2: register the MapReduce class
        job.setJarByClass(MaxLineLen.class);  

        // Step 3:  Set Input and Output files
        FileInputFormat.setInputPaths(job, new Path("sense.txt")); 
        FileOutputFormat.setOutputPath(job, new Path("maxLine-out")); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job.setMapperClass(MaxLenMapper.class);
        job.setReducerClass(MaxLenReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Longest Sentence - Holly Haraguchi");

        // Step 7
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}

