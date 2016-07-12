package com.yyb.inputoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by yyb on 16-7-12.
 */
public class RandomDataGenerationDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        int numMapTasks = Integer.parseInt(args[0]);
        int numRecodrsPerTasks = Integer.parseInt(args[1]);
        Path wordList = new Path(args[2]);
        Path outputDir = new Path(args[3]);

        Job job = Job.getInstance(conf, "RandomDataGenerationDriver");
        job.setJarByClass(RandomDataGenerationDriver.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(RandomStackOverflowInputFormat.class);

        RandomStackOverflowInputFormat.setNumMapTasks(job, numMapTasks);
        RandomStackOverflowInputFormat.setNumRecordPerTask(job, numRecodrsPerTasks);
        RandomStackOverflowInputFormat.setRandomWordList(job, wordList);
        TextOutputFormat.setOutputPath(job, outputDir);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class RandomStackOverflowInputFormat extends InputFormat<Text, NullWritable> {


        public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
        public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";
        public static final String RANDOM_WORD_LIST = "random.generator.random.word.file";

        @Override
        public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

            int numSplits = context.getConfiguration().getInt(NUM_MAP_TASKS, -1);

            ArrayList<InputSplit> splits = new ArrayList<>();
            for (int i = 0; i < numSplits; i++) {
                splits.add(new FakeInputSplit());
            }
            return splits;
        }

        @Override
        public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            RandomStackOverflowRecordReader rr = new RandomStackOverflowRecordReader();
            rr.initialize(split, context);
            return rr;
        }

        public static void setNumMapTasks(Job job, int i) {
            job.getConfiguration().setInt(NUM_MAP_TASKS, i);
        }

        public static void setNumRecordPerTask(Job job, int i) {
            job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, i);
        }

        public static void setRandomWordList(Job job, Path file) {
            job.addCacheFile(file.toUri());
        }


        public static class RandomStackOverflowRecordReader extends RecordReader<Text, NullWritable> {

            private int numRecordsToCreate = 0;
            private int createdRecords = 0;
            private Text key = new Text();
            private NullWritable value = NullWritable.get();
            private Random random = new Random();
            private ArrayList<String> randomWords = new ArrayList<>();
            private SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {

                // Get the number of records to create from the configuration
                this.numRecordsToCreate = context.getConfiguration().getInt(NUM_RECORDS_PER_TASK, -1);

                // Get the list of random words from the DistributedCache
                URI[] files = context.getCacheFiles();

                BufferedReader br = new BufferedReader(new FileReader(files[0].toString()));

                String line;
                while ((line = br.readLine()) != null) {
                    randomWords.add(line);
                }

                br.close();
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (createdRecords < numRecordsToCreate) {
                    int score = Math.abs(random.nextInt()) % 15000;
                    int rowId = Math.abs(random.nextInt()) % 1000000000;
                    int postId = Math.abs(random.nextInt()) % 100000000;
                    int userId = Math.abs(random.nextInt()) % 1000000;
                    String creationDate = frmt.format(Math.abs(random.nextLong()));

                    String text = getRandomText();

                    String randomRecord = "<row Id=\"" + rowId + "\" PostId\"" + postId + "\" Score=\"" + score
                            + "\" Text=\"" + text + "\" CreationDate\"" + creationDate + "\" UserId=\"" + userId + "\" />";
                    key.set(randomRecord);
                    ++createdRecords;
                    return true;
                }
                return false;
            }

            private String getRandomText() {
                StringBuilder sb = new StringBuilder();
                int numWords = Math.abs(random.nextInt()) % 30 + 1;

                for (int i = 0; i < numWords; i++) {
                    sb.append(randomWords.get(Math.abs(random.nextInt()) % randomWords.size()) + " ");
                }
                return sb.toString();
            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return key;
            }

            @Override
            public NullWritable getCurrentValue() throws IOException, InterruptedException {
                return value;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return (float) createdRecords / (float) numRecordsToCreate;
            }

            @Override
            public void close() throws IOException {
                // nothing to do here ...
            }
        }

    }

    public static class FakeInputSplit extends InputSplit implements Writable {
        @Override
        public void write(DataOutput out) throws IOException {

        }

        @Override
        public void readFields(DataInput in) throws IOException {

        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }
    }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new RandomDataGenerationDriver(), args));
    }
}




