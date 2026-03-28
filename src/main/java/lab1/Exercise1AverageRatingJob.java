package lab1;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Exercise1AverageRatingJob {
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text movieId = new Text();
        private final DoubleWritable rating = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 4);
            if (parts.length < 4) {
                return;
            }

            movieId.set(parts[1].trim());
            rating.set(Double.parseDouble(parts[2].trim()));
            context.write(movieId, rating);
        }
    }

    public static class RatingReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
        private Map<String, String> movieTitles;
        private String maxMovie;
        private double maxRating = Double.NEGATIVE_INFINITY;
        private int maxCount;

        @Override
        protected void setup(Context context) throws IOException {
            movieTitles = MovieLookup.loadTitles(context.getConfiguration(), context.getCacheFiles());
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            double average = sum / count;
            String title = movieTitles.getOrDefault(key.toString(), "Unknown Movie (" + key + ")");
            String line = String.format(Locale.US, "%s AverageRating: %.2f (TotalRatings: %d)", title, average, count);
            context.write(new Text(line), NullWritable.get());

            if (count >= 5 && average > maxRating) {
                maxMovie = title;
                maxRating = average;
                maxCount = count;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String line;
            if (maxMovie == null) {
                line = "No movie has at least 5 ratings in the provided dataset.";
            } else {
                line = String.format(
                        Locale.US,
                        "%s is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings (%d ratings).",
                        maxMovie,
                        maxRating,
                        maxCount);
            }
            context.write(new Text(line), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Exercise1AverageRatingJob <movies> <ratings1> <ratings2> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "exercise-1-average-rating");
        job.setJarByClass(Exercise1AverageRatingJob.class);

        job.addCacheFile(new Path(args[0]).toUri());
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
