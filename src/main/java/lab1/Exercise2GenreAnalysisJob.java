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

public class Exercise2GenreAnalysisJob {
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text genre = new Text();
        private final DoubleWritable ratingWritable = new DoubleWritable();
        private Map<String, String[]> movieGenres;

        @Override
        protected void setup(Context context) throws IOException {
            movieGenres = MovieLookup.loadGenres(context.getConfiguration(), context.getCacheFiles());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 4);
            if (parts.length < 4) {
                return;
            }

            String[] genres = movieGenres.get(parts[1].trim());
            if (genres == null) {
                return;
            }

            ratingWritable.set(Double.parseDouble(parts[2].trim()));
            for (String item : genres) {
                genre.set(item.trim());
                context.write(genre, ratingWritable);
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            String line = String.format(Locale.US, "%s: %.2f (TotalRatings: %d)", key.toString(), sum / count, count);
            context.write(new Text(line), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Exercise2GenreAnalysisJob <movies> <ratings1> <ratings2> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "exercise-2-genre-analysis");
        job.setJarByClass(Exercise2GenreAnalysisJob.class);

        job.addCacheFile(new Path(args[0]).toUri());
        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

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
