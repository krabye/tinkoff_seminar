import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Task1Job extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static class Task1MapperStep1 extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String uid = split[0];
            String time = split[1];
            String url = split[2];

            String date_hour = time.split(":")[0];

            String host = "";
            try {
                host = new URI(url).getHost();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

            context.write(new Text(host + "\t" + date_hour), new LongWritable(1));
        }
    }

    public static class Task1ReducerStep1 extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> visits, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable i: visits)
                count += i.get();

            context.write(key, new LongWritable(count));
        }
    }

    public static class Task1MapperStep2 extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String uid = split[0];
            String time = split[1];
            String url = split[2];

            String date_hour = time.split(":")[0];

            String host = "";
            try {
                host = new URI(url).getHost();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

            context.write(new Text(host + "\t" + date_hour), new LongWritable(1));
        }
    }
}
