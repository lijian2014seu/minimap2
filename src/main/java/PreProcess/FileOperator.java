package PreProcess;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class FileOperator {

    public static void deleteFile(File file){
        if (file == null || !file.exists()){
            System.out.println("delete failed");
            return;
        }
        File[] files = file.listFiles();
        for (File f: files){
            if (f.isDirectory()){
                deleteFile(f);
            }else {
                f.delete();
            }
        }
        file.delete();
    }

    public static void DeleteHadoopFile(String uri,String ip){
        String port=":9000";
        String addr="hdfs://"+ip+port;
        Configuration conf = new Configuration();
        try {
            Properties properties = System.getProperties();
            properties.setProperty("hadoop", "root");
            conf.set("fs.defaultFS", addr);
            FileSystem fs = FileSystem.get(conf);
            Path delPath = new Path(uri);
            boolean isDeleted = fs.delete(delPath,true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String gzfile(String inputName, String ip){
        String port=":9000";
        String addr="hdfs://"+ip+port;
        Configuration conf = new Configuration();
        try {
            Properties properties = System.getProperties();
            properties.setProperty("hadoop", "root");
            conf.set("fs.defaultFS", addr);
            FileSystem fs = FileSystem.get(conf);
            BufferedInputStream buf= null;
            FSDataInputStream hdfsin=fs.open(new Path(inputName));
            GzipCompressorInputStream gzip = new GzipCompressorInputStream(new BufferedInputStream(hdfsin));
            String name = inputName.substring(inputName.lastIndexOf("/")+1).replace(".gz","");
            String fileName = inputName.substring(0,inputName.lastIndexOf("/"))+"/"+name;
            DeleteHadoopFile(fileName,ip);
            FSDataOutputStream hdfsOutStream = fs.create(new Path(fileName));
            BufferedOutputStream outfile = new BufferedOutputStream(hdfsOutStream);
            int b;
            while ((b = gzip.read()) != -1) {
//                System.out.println("buf read"+gzip.read());
                outfile.write(b);
            }
            outfile.flush();
            outfile.close();
            return fileName;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
