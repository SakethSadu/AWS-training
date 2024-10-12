import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

public class EnvLoader {
    public static void loadEnv(String envFilePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(envFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Ignoring comments and empty lines
                if (line.trim().isEmpty() || line.startsWith("#")) {
                    continue;
                }
                
              
                String[] keyValue = line.split("=", 2);
                if (keyValue.length == 2) {
                    // Setting the environment variable
                    System.setProperty(keyValue[0].trim(), keyValue[1].trim());
                }
            }
        }
    }
}
