package applications.applicationLogs;

import master.Master;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static utils.FileUtil.sparkOutputCompareUtil;

public class ApplicationLogs {
    public static void main(String[] args) throws IOException, InterruptedException {
        Boolean induceFault = Boolean.parseBoolean(args[0]);

        Master master = new Master();
        System.out.println("Starting Application Logs example");
        System.out.println("---------------------------------------------------------------");
        System.out.println("Map and Reduce function in: ApplicationLogsMapReduce.java");
        System.out.println("Input file: applicationLogsInput.txt\nOutput directory: output");

        Properties properties = new Properties();
        try (InputStream input = new FileInputStream("resources/applicationLogsConfig.properties")) {
            // load a properties file
            properties.load(input);
            properties.put("induceFault", induceFault);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        master.mapReduce(properties);

        try {
            if (sparkOutputCompareUtil("output/application_logs_output", "spark_outputs/applicationLogs.txt")) {
                System.out.println("Tests passed - Comparison with actual spark for application logs returned no differences.");
            } else {
                System.out.println("Tests failed - Comparison with actual spark for application logs returned differences.");
            }
        } catch (Exception e) {
            System.out.println("Error");
        }
    }
}
