import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UrlParamsCounter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1]+"/unique_urls";
        Job job1 = getJobConf1(input, output);
        if (!job1.waitForCompletion(true))
            return 1;

        input = output + "/part*";
        output = args[1]+"/param_counts";
        Job job2= getJobConf2(input, output);
        if (!job2.waitForCompletion(true))
            return 1;

        return 0;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new UrlParamsCounter(), args);
        System.exit(ret);
    }

    private Job getJobConf1(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(UrlParamsCounter.class);
        job.setJobName(UrlParamsCounter.class.getCanonicalName());

        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(MapperUnique.class);
        job.setReducerClass(ReducerUnique.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    private Job getJobConf2(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(UrlParamsCounter.class);
        job.setJobName(UrlParamsCounter.class.getCanonicalName());

        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(MapperCount.class);
        job.setCombinerClass(ReducerCount.class);
        job.setReducerClass(ReducerCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job;
    }

    public static class MapperUnique extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t", 3);
            String url = split[2];
            Pattern pattern = Pattern.compile("^(([^:/?#]+):)?(/?/?([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
            Matcher matcher = pattern.matcher(url);

            if (!matcher.find()) {
                context.getCounter("COMMON_COUNTERS", "SkippedUrls").increment(1);
                return;
            }

            String host = matcher.group(4);
            String path = matcher.group(5);
            String query = matcher.group(7);

            if (query == null) {
                context.getCounter("COMMON_COUNTERS", "NoQuery").increment(1);
                return;
            }

            for (String pair : query.split("&"))
                if (!pair.equals("=") && pair.contains("="))
                    context.write(
                            new Text(host + path),
                            new Text(pair.split("=")[0])
                    );
        }
    }

    public static class ReducerUnique extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> params, Context context) throws IOException, InterruptedException {
            HashSet<Text> params_set = new HashSet<>();
            for (Text param: params)
                params_set.add(param);

            for (Text param: params_set)
                context.write(key, param);
        }
    }

    public static class MapperCount extends Mapper<LongWritable, Text, Text, LongWritable> {
        static final LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            if (split.length < 2)
                return;

            context.write(new Text(split[1]), one);
        }
    }

    public static class ReducerCount extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable i: values)
                count += i.get();

            context.write(key, new LongWritable(count));
        }
    }
}
