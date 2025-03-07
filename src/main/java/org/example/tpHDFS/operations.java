package org.example.tpHDFS;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import java.io.*;

public class operations {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://namenode:8020");
        FileSystem fs = FileSystem.get(conf);

        // 1. Create directory if not exists
        Path dirPath = new Path("/user/hadoop/appData");
        if (!fs.exists(dirPath)) {
            fs.mkdirs(dirPath);
            System.out.println("Directory created: " + dirPath);
        }

        // 2. List all files and directories
        System.out.println("Listing files in: " + dirPath);
        FileStatus[] statuses = fs.listStatus(dirPath);
        for (FileStatus status : statuses) {
            System.out.println(status.getPath().toString());
        }

        // 3. Create file and write text
        Path filePath = new Path("/user/hadoop/appData/data.txt");
        FSDataOutputStream outputStream = fs.create(filePath);
        outputStream.writeBytes("Bienvenue sur HDFS avec Java.\n");
        outputStream.close();
        System.out.println("File created: " + filePath);

        // 4. Read and display file content
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
        String line;
        System.out.println("Reading file content:");
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();

        // 5. Copy a local file to HDFS
        Path localPath = new Path("test.txt");
        Path hdfsPath = new Path("/user/hadoop/appData/test.txt");
        fs.copyFromLocalFile(localPath, hdfsPath);
        System.out.println("File copied to HDFS: " + hdfsPath);

        // 6. Download file from HDFS to local system
        Path localDownloadPath = new Path("downloaded_test.txt");
        fs.copyToLocalFile(hdfsPath, localDownloadPath);
        System.out.println("File downloaded to local system: " + localDownloadPath);

        // 7. Rename file
        Path renamedPath = new Path("/user/hadoop/appData/data_v1.txt");
        fs.rename(filePath, renamedPath);
        System.out.println("File renamed to: " + renamedPath);

        // 8. Delete file
        if (fs.delete(renamedPath, false)) {
            System.out.println("File deleted: " + renamedPath);
        }

        // 9. Get metadata of file
        FileStatus fileStatus = fs.getFileStatus(hdfsPath);
        System.out.println("Metadata of file " + hdfsPath + ":");
        System.out.println("Size: " + fileStatus.getLen());
        System.out.println("Owner: " + fileStatus.getOwner());
        System.out.println("Permissions: " + fileStatus.getPermission());

        // 10. Check available space
        FsStatus fsStatus = fs.getStatus();
        System.out.println("HDFS Available Space: " + fsStatus.getRemaining() + " bytes");

        // 11. Move file to subdirectory
        Path archiveDir = new Path("/user/hadoop/archive");
        if (!fs.exists(archiveDir)) {
            fs.mkdirs(archiveDir);
        }
        Path movedFilePath = new Path("/user/hadoop/archive/test.txt");
        fs.rename(hdfsPath, movedFilePath);
        System.out.println("File moved to: " + movedFilePath);

        fs.close();
    }
}
