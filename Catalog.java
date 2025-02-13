import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Catalog {
    // Some further catalog info I have in the README.

    public static void readTableSchema(String tableName) {
        // Return record schema for given table name

        String filePath = "path/to/catalog/"; // TODO: Somehow we need to determine the file path
        try (FileInputStream fileInputStream = new FileInputStream(filePath);
                FileChannel fileChannel = fileInputStream.getChannel()) {

            long fileSize = fileChannel.size();
            ByteBuffer bbuffer = ByteBuffer.allocate((int) fileSize); // Allocate buffer

            fileChannel.read(bbuffer); // Read catalog data into buffer
            bbuffer.rewind();

            // Process the data from the buffer
            while (bbuffer.hasRemaining()) {
                int tableID = bbuffer.get(); // Convert first byte to int, <table_id>
                int bytes_to_read = bbuffer.get();

                // TODO: Process table schema
                // Search table for requested ID!
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeTableSchema(String tableName, int[] tableSchema) {
        // Add new table to catalog
    }

}
