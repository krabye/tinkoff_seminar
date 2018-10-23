import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class UrlPathsCounter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1]+"/unique_urls";
        Job job1 = getJobConf1(input, output);
        if (!job1.waitForCompletion(true))
            return 1;

        input = output + "/part*";
        output = args[1]+"/url_counts";
        Job job2= getJobConf2(input, output);
        if (!job2.waitForCompletion(true))
            return 1;

        return 0;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new UrlPathsCounter(), args);
        System.exit(ret);
    }

    private Job getJobConf1(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(UrlPathsCounter.class);
        job.setJobName(UrlPathsCounter.class.getCanonicalName());

        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(MapperUnique.class);
        job.setReducerClass(ReducerUnique.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job;
    }

    private Job getJobConf2(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(UrlPathsCounter.class);
        job.setJobName(UrlPathsCounter.class.getCanonicalName());

        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(MapperCount.class);
        job.setCombinerClass(ReducerCount.class);
        job.setReducerClass(ReducerCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job;
    }

    public static class MapperUnique extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t", 3);
            String url = split[2];

            context.write(new Text(url), NullWritable.get());
        }
    }

    public static class ReducerUnique extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> visits, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class MapperCount extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String url = value.toString().trim();
            URI uri = null;

            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            assert uri == null;

            String path = uri.getRawPath();

            context.write(new Text(path), new LongWritable(1));
        }
    }

    public static class ReducerCount extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> visits, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable i: visits)
                count += i.get();

            context.write(key, new LongWritable(count));
        }
    }
}
