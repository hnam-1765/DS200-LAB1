package lab1;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Exercise4AgeGroupAnalysisJob {
    public static class AgeGroupMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text movieId = new Text();
        private final Text ageGroupAndRating = new Text();
        private Map<String, Integer> users;

        @Override
        protected void setup(Context context) throws IOException {
            users = UserLookup.loadAges(context.getConfiguration(), context.getCacheFiles());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 4);
            if (parts.length < 4) {
                return;
            }

            Integer age = users.get(parts[0].trim());
            if (age == null) {
                return;
            }

            movieId.set(parts[1].trim());
            ageGroupAndRating.set(toAgeGroup(age) + ":" + parts[2].trim());
            context.write(movieId, ageGroupAndRating);
        }

        private String toAgeGroup(int age) {
            if (age < 18) {
                return "0-18";
            }
            if (age < 35) {
                return "18-35";
            }
            if (age < 50) {
                return "35-50";
            }
            return "50+";
        }
    }

    public static class AgeGroupReducer extends Reducer<Text, Text, Text, NullWritable> {
        private static final String[] AGE_GROUPS = {"0-18", "18-35", "35-50", "50+"};
        private Map<String, String> movieTitles;

        @Override
        protected void setup(Context context) throws IOException {
            movieTitles = MovieLookup.loadTitles(context.getConfiguration(), context.getCacheFiles());
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, double[]> stats = new LinkedHashMap<>();
            for (String ageGroup : AGE_GROUPS) {
                stats.put(ageGroup, new double[] {0.0, 0.0});
            }

            for (Text value : values) {
                String[] parts = value.toString().split(":", 2);
                if (parts.length < 2) {
                    continue;
                }

                double[] stat = stats.get(parts[0]);
                if (stat == null) {
                    continue;
                }

                stat[0] += Double.parseDouble(parts[1]);
                stat[1] += 1;
            }

            StringBuilder builder = new StringBuilder();
            builder.append(movieTitles.getOrDefault(key.toString(), "Unknown Movie (" + key + ")"));
            builder.append(": [");

            for (int i = 0; i < AGE_GROUPS.length; i++) {
                String ageGroup = AGE_GROUPS[i];
                double[] stat = stats.get(ageGroup);

                builder.append(ageGroup).append(": ");
                if (stat[1] == 0) {
                    builder.append("N/A");
                } else {
                    builder.append(String.format(Locale.US, "%.2f", stat[0] / stat[1]));
                }

                if (i < AGE_GROUPS.length - 1) {
                    builder.append(", ");
                }
            }

            builder.append("]");
            context.write(new Text(builder.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: Exercise4AgeGroupAnalysisJob <movies> <users> <ratings1> <ratings2> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "exercise-4-age-group-analysis");
        job.setJarByClass(Exercise4AgeGroupAnalysisJob.class);

        job.addCacheFile(new Path(args[0]).toUri());
        job.addCacheFile(new Path(args[1]).toUri());
        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
