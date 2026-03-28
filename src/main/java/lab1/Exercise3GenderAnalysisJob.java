package lab1;

import java.io.IOException;
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

public class Exercise3GenderAnalysisJob {
    public static class GenderMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text movieId = new Text();
        private final Text genderAndRating = new Text();
        private Map<String, String> users;

        @Override
        protected void setup(Context context) throws IOException {
            users = UserLookup.loadGenders(context.getConfiguration(), context.getCacheFiles());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 4);
            if (parts.length < 4) {
                return;
            }

            String gender = users.get(parts[0].trim());
            if (gender == null) {
                return;
            }

            movieId.set(parts[1].trim());
            genderAndRating.set(gender + ":" + parts[2].trim());
            context.write(movieId, genderAndRating);
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, NullWritable> {
        private Map<String, String> movieTitles;

        @Override
        protected void setup(Context context) throws IOException {
            movieTitles = MovieLookup.loadTitles(context.getConfiguration(), context.getCacheFiles());
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double maleSum = 0.0;
            int maleCount = 0;
            double femaleSum = 0.0;
            int femaleCount = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(":", 2);
                if (parts.length < 2) {
                    continue;
                }

                double rating = Double.parseDouble(parts[1]);
                if ("M".equalsIgnoreCase(parts[0])) {
                    maleSum += rating;
                    maleCount++;
                } else if ("F".equalsIgnoreCase(parts[0])) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            String title = movieTitles.getOrDefault(key.toString(), "Unknown Movie (" + key + ")");
            String maleAvg = maleCount == 0 ? "N/A" : String.format(Locale.US, "%.2f", maleSum / maleCount);
            String femaleAvg = femaleCount == 0 ? "N/A" : String.format(Locale.US, "%.2f", femaleSum / femaleCount);

            context.write(
                    new Text(String.format(Locale.US, "%s: Male_Avg=%s, Female_Avg=%s", title, maleAvg, femaleAvg)),
                    NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: Exercise3GenderAnalysisJob <movies> <users> <ratings1> <ratings2> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "exercise-3-gender-analysis");
        job.setJarByClass(Exercise3GenderAnalysisJob.class);

        job.addCacheFile(new Path(args[0]).toUri());
        job.addCacheFile(new Path(args[1]).toUri());
        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

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
