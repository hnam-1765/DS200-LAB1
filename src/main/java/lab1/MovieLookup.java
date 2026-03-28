package lab1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class MovieLookup {
    private MovieLookup() {
    }

    public static Map<String, String> loadTitles(Configuration conf, URI[] cacheFiles) throws IOException {
        Map<String, String> movies = new HashMap<>();
        if (cacheFiles == null) {
            return movies;
        }

        for (URI cacheFile : cacheFiles) {
            Path path = new Path(cacheFile.getPath());
            if (!path.getName().equals("movies.txt")) {
                continue;
            }

            FileSystem fs = path.getFileSystem(conf);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 3);
                    if (parts.length < 3) {
                        continue;
                    }
                    movies.put(parts[0].trim(), parts[1].trim());
                }
            }
        }

        return movies;
    }

    public static Map<String, String[]> loadGenres(Configuration conf, URI[] cacheFiles) throws IOException {
        Map<String, String[]> genresByMovie = new HashMap<>();
        if (cacheFiles == null) {
            return genresByMovie;
        }

        for (URI cacheFile : cacheFiles) {
            Path path = new Path(cacheFile.getPath());
            if (!path.getName().equals("movies.txt")) {
                continue;
            }

            FileSystem fs = path.getFileSystem(conf);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 3);
                    if (parts.length < 3) {
                        continue;
                    }
                    genresByMovie.put(parts[0].trim(), parts[2].trim().split("\\|"));
                }
            }
        }

        return genresByMovie;
    }
}
