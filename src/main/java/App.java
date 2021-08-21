import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class App {

    public static void main(String[] args) throws EventHubException, IOException {

        System.out.println("Azure Storage Account, blob storage Sample App");
        String connectStr = "DefaultEndpointsProtocol=https;AccountName=Your_Acc_Name;AccountKey=Your_Acc_Key;EndpointSuffix=core.windows.net";
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectStr).buildClient();
            String containerName = "Your_Contaner_Name";  // Where you want to upload your file

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient blobClient;

        final ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder()
                .setNamespaceName("Your_EventHub_NameSpace")
                .setEventHubName("Your_EventHubName")
                .setSasKeyName("YouSaSKeyName_Ex-RootManageSharedAccessPolicy")
                .setSasKey("YourSasKey;EntityPath=Your_EventHubName");

        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
        final EventHubClient eventHubClient = EventHubClient.createFromConnectionStringSync(connectionStringBuilder.toString(), executorService);

        String localPath = "Your_Local_Path_Where_You_Want to_Create_Your_File_To_Upload_to_Blob""; //Ex: D:\\WorkSpace\\PoCAzureSubscriber\\src\\data
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println("Time of File creation : " + dtf.format(now));
        String fileName = "File_Name_Prefix" + dtf.format(now) + ".txt";
        File localFile = new File(localPath + fileName);
        System.out.println("Name and path of the file: . . " + localPath+fileName);
        FileWriter writer = new FileWriter(localPath + fileName, true);
        System.out.println("File name to be uploaded : " + fileName);
        writer.write("Hello, World!");
        writer.close();

        blobClient = containerClient.getBlobClient(fileName);
        System.out.println("\nUploading to Blob storage as blob:\n\t" + blobClient.getBlobUrl());
        blobClient.uploadFromFile(localPath + fileName);
        System.out.println("\nListing blobs...");
        localFile.delete();

        // List the blob(s) in the container.
        for (BlobItem blobItem : containerClient.listBlobs()) {
            System.out.println("\t" + blobItem.getName());
        }

        final Gson gson = new GsonBuilder().create();
        try {
            if (!fileName.isEmpty()) {
                String payLoad = fileName;
                byte[] payLoadBytes = gson.toJson(payLoad).getBytes(Charset.defaultCharset());
                EventData sendData = EventData.create(payLoadBytes);
                System.out.println("Sending Event . . . . . .");
                eventHubClient.send(sendData);
            }
            System.out.println(Instant.now() + " Send Event Complete . . . . . .");
            System.out.println("Press Eneter to Stop");
            System.in.read();
        }   
        finally {
            eventHubClient.closeSync();
            executorService.shutdown();
        }
    }
}
