package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

// WordCountCache exposes the WordCount MapReduce implementation
// making use of a Distributed Cache.
// The distributed cache is a file which defines the list of
// words for which the word count needs to be extracted using MapReduce.
public class WordCountCache {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set wordList = new HashSet();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {

                // Extract the cache file if set in the configuration.
                Path[] wordListFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

                if (wordListFiles != null && wordListFiles.length > 0) {
                    for (Path wordListFile: wordListFiles) {
                       scanFile(wordListFile);
                    }
                }
            } catch (IOException ex) {
                System.err.println("Exception in reading Distributed File: " + ex.getMessage());
            }
        }

        private void scanFile(Path path) {
            try {
                BufferedReader buff = new BufferedReader(new FileReader(path.toString()));
                String line = null;

                // 1. Read the file.
                // 2. Extract each line in the file and extract tokens
                //    by splitting each line by a space.
                // 3. Write the tokens in a HashSet in order to retain only
                //    unique words from the cache file.
                while((line = buff.readLine()) != null) {
                    String[] tokens = line.split(" ");
                    for (int i = 0; i < tokens.length; i++) {
                        wordList.add(tokens[i].toLowerCase());
                    }

                }
            } catch (IOException ex) {
                System.err.println("IOException for path: " + path + " Message: " + ex.getMessage());
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            for (int i = 0; i < tokens.length; i++) {
                // If the word exists in the Hashset, then set its
                // count to one and write it.
                if (wordList.contains(tokens[i])) {
                    context.write(new Text(tokens[i]), one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count cache");
        job.setJarByClass(WordCountCache.class);
        job.setMapperClass(WordCountCache.TokenizerMapper.class);
        job.setCombinerClass(WordCountCache.IntSumReducer.class);
        job.setReducerClass(WordCountCache.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (args.length == 3) {
            DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
