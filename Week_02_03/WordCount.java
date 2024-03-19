// dit schrijft onderstaande cell naar een file in de huidige directory
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // Mapper de classes na extends Mapper<> zijn input-key, input-value, output-key, output-value
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // value bevat de tekst in de huidige blok
            // de StringTokenizer maakt het mogelijk om over elk woord te lopen
            StringTokenizer itr = new StringTokenizer(value.toString());
            // while-loop over alle woorden in de blocks op elke node
            while (itr.hasMoreTokens()) {
                // convert the token naar een text waarde
                word.set(itr.nextToken());
                // voeg een entry toe voor elk woord dat gezien wordt (deze worden in de reduce opgeteld)
                context.write(word, one);
            }
        }
    }

    // Reducer: de classes na extends Reducer<> zijn input-key, input-value, output-key, output-value
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // configure the MapReduce program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count java");
        job.setJarByClass(WordCount.class);
        // configure mapper
        job.setMapperClass(TokenizerMapper.class);
        // configure combiner (soort van reducer die draait op mapping node voor performantie)
        job.setCombinerClass(IntSumReducer.class);
        // configure reducer
        job.setReducerClass(IntSumReducer.class);
        // set output key-value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // set input file (first argument passed to the program)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set output file  (second argument passed to the program)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // In this case, we wait for completion to get the output/logs and not stop the program to early.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
