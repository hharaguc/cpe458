// Holly Haraguchi
// CPE 458, Winter 2017
// Lab 11, Part 2
// Outputs the longest sentence in "The Irish Penny Journal"

// Section 1: Imports
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object

// Exception handling
import java.io.IOException;

public class MaxLineLen {
    // Mapper Class
    public static class MaxLenMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Regex found here: http://stackoverflow.com/questions/28554260/split-text-with-quotes-into-sentences-using-breakiterator-java
            String[] sentences = value.toString().split("(?<!Mrs?\\.)(?<=\\.)\\s+(?=(?:\"[^\"]*\"|[^\"])*$)");
            
            // Output each sentence found with an arbitrary key
            for (String sent : sentences) {
                context.write(new Text("key"), new Text(sent + "///" + sent.replace(" " , "").length()));   
            }

        } 
    }

    // Reducer Class
    public static class MaxLenReducer extends Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxLen = 0;
            String maxSent = "";

            for (Text val : values) {
                String[] splits = val.toString().split("///");
                int curLen = Integer.parseInt(splits[1]);
                if (curLen > maxLen) {
                    maxLen = curLen;
                    maxSent = splits[0];
                }
            }
            
            // Output the file's longest line + line length
            context.write(new Text(maxSent), new Text(Integer.toString(maxLen)));
      }
    }


    // Section 4:  MapReduce Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Step 1: get a new MapReduce Job object
        Job job = Job.getInstance(conf);  

        // Step 2: register the MapReduce class
        job.setJarByClass(MaxLineLen.class);  

        // Step 3:  Set Input and Output files
        FileInputFormat.setInputPaths(job, new Path("pride.txt")); 
        FileOutputFormat.setOutputPath(job, new Path("maxLine-out")); 

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

