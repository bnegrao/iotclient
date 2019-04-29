import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.time.format.DateTimeFormatter;
import java.util.zip.GZIPOutputStream;
import java.util.Properties;

public class IotClient {

    private static Properties prop;

    static {
        try(InputStream input = IotClient.class.getClassLoader().getResourceAsStream("config.properties")){
            prop = new Properties();

            if (input == null) {
                throw new RuntimeException("Sorry, unable to find config.properties");
            }

            prop.load(input);
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    public static class Message {
        public Message(String message) {
            this.message = message;
        }

        String message;
    }

    public static void main(String[] args) throws Exception {
        AWSIotMqttClient client = getClientForWebsocket();

        // optional parameters can be set before connect()
        client.connect();

        String[] topics = prop.getProperty("topics").split(",");

        for (String topic : topics) {
            String[] jsonMessageFiles = prop.getProperty("jsonFiles").split(",");
            String dir = prop.getProperty("jsonFilesDirectory");
            for (String jsonMessageFile : jsonMessageFiles) {

                JSONObject jsonObject = readJsonFile(dir + jsonMessageFile);

                jsonObject.replace("date", getFormattedDate());

                byte[] compressed = compress(jsonObject.toString());

                client.publish(topic, AWSIotQos.QOS0, compressed);
            }
        }

        client.disconnect();
    }


    private static byte[] compress(String str) throws Exception {
        if (str == null || str.length() == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(str.getBytes());
        gzip.close();
        return out.toByteArray();
    }


    private static String getFormattedDate() {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;


        return formatter.format(java.time.Instant.now());
    }


    private static JSONObject readJsonFile(String filename) throws Exception {
        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();

        FileReader reader = new FileReader(filename);

        //Read JSON file
        Object obj = jsonParser.parse(reader);

        JSONObject pdvEvent = (JSONObject) obj;
        System.out.println(pdvEvent);

        return pdvEvent;

    }


    private static AWSIotMqttClient getClientForWebsocket() throws IOException {

        String clientEndpoint = prop.getProperty("clientEndpoint");
        String clientId = prop.getProperty("clientId");
        String awsAccessKeyId = prop.getProperty("awsAccessKeyId");
        String awsSecretAccessKey = prop.getProperty("awsSecretAccessKey");

        return new AWSIotMqttClient(clientEndpoint, clientId, awsAccessKeyId, awsSecretAccessKey);
    }
}
