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

public final class UserLookup {
    private UserLookup() {
    }

    public static Map<String, String> loadGenders(Configuration conf, URI[] cacheFiles) throws IOException {
        Map<String, String> genders = new HashMap<>();
        if (cacheFiles == null) {
            return genders;
        }

        for (URI cacheFile : cacheFiles) {
            Path path = new Path(cacheFile.getPath());
            if (!path.getName().equals("users.txt")) {
                continue;
            }

            FileSystem fs = path.getFileSystem(conf);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 5);
                    if (parts.length < 5) {
                        continue;
                    }
                    genders.put(parts[0].trim(), parts[1].trim());
                }
            }
        }

        return genders;
    }

    public static Map<String, Integer> loadAges(Configuration conf, URI[] cacheFiles) throws IOException {
        Map<String, Integer> ages = new HashMap<>();
        if (cacheFiles == null) {
            return ages;
        }

        for (URI cacheFile : cacheFiles) {
            Path path = new Path(cacheFile.getPath());
            if (!path.getName().equals("users.txt")) {
                continue;
            }

            FileSystem fs = path.getFileSystem(conf);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 5);
                    if (parts.length < 5) {
                        continue;
                    }
                    ages.put(parts[0].trim(), Integer.parseInt(parts[2].trim()));
                }
            }
        }

        return ages;
    }
}
