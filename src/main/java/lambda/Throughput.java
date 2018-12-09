/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import faasinspector.register;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

/**
 *
 * @author frank
 */
public class Throughput {
    String m_bucketName;
    String m_fileName;
    String m_fileNameLocal;
    String m_bucketNameMid;
    String m_fileNameMid;
    String m_bucketNameDB;
    String m_fileNameDB;
    String m_localPath;
    String m_localFullPath;
    String m_jdbcName;
    String m_aggregation;
    String m_filter;
    
    LambdaLogger m_logger;
    AmazonS3 m_s3Client;
    
    void Init(Request request, Context context) {
        m_bucketName = request.getBucketname();
        m_fileName = request.getFilename();
        m_fileNameLocal = "/tmp/"+m_fileName+".mid.csv";;
        m_bucketNameMid = "fg.tmp.files";
        m_fileNameMid = m_fileName+".mid.csv";
        m_bucketNameDB = "fg.db.files";
        m_fileNameDB = m_fileName+".db";
        m_localPath = m_fileNameDB;
        m_localFullPath = "/tmp/"+m_localPath;
        m_jdbcName = "jdbc:sqlite:"+m_fileNameDB;
        m_aggregation = request.getAggregation();
        m_filter = request.getFilter();
        
//        setCurrentDirectory("/tmp");
        m_logger = context.getLogger();
        m_s3Client = AmazonS3ClientBuilder.standard().build();
    }
    
    public Response handleRequest(Request request, Context context) {
        long t1 = System.currentTimeMillis();
        
        register reg = new register(m_logger);
        Response r = reg.StampContainer();
        Init(request, context);
        
        m_logger.log("2");
        
        HashSet<String> orderSet = new HashSet();
        
        S3Object s3Object = null;
        try {
            s3Object = m_s3Client.getObject(new GetObjectRequest(m_bucketName, m_fileName));
        }
        catch (SdkClientException e) {
        }
        
        if( s3Object == null ) {
            m_logger.log("s3Object is null");
            return r;
        }
        else {
            long file_size = s3Object.getObjectMetadata().getContentLength();
            m_logger.log("s3Object is not null, size:"+file_size);
        }
        InputStream objectData = s3Object.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
        
        int count = 0;
        String line = "";
        StringBuilder sb = new StringBuilder();
        try
        {
//            long t1 = System.currentTimeMillis();
            File localFile = new File(m_fileNameLocal);
            BufferedWriter bw = new BufferedWriter(new FileWriter(localFile));
            
            SimpleDateFormat fdateFrmat = new SimpleDateFormat("MM/dd/yyyy");
            while (true) {
                line = reader.readLine();
                if( count==0 ) {
                    count++;
                    continue;
                }
                if (line == null) break;
                line=line.replace("\'", "\'\'");
                String[] items = line.split(",");
                if( orderSet.contains(items[6]) ) {
                    continue;
                }
                orderSet.add(items[6]);
                
                for(int i=0;i<4;++i) {
                    sb.append("\'"+items[i]);
                    sb.append("\',");
                }
                if( items[4].equals("L") )
                    sb.append("\'Low\'");
                else if( items[4].equals("M") )
                    sb.append("\'Medium\'");
                else if( items[4].equals("H") )
                    sb.append("\'High\'");
                else if( items[4].equals("C") )
                    sb.append("\'Critical\'");
                sb.append(",");

                try {
                    Date orderdate = fdateFrmat.parse(items[5]);
                    Date shipdate = fdateFrmat.parse(items[7]);
                    
                    sb.append((orderdate.getTime()/1000)+",");
                    sb.append(items[6]);
                    sb.append(",");
                    sb.append((shipdate.getTime()/1000)+",");
                    
                    for(int i=8;i<14;++i) {
                        sb.append(items[i]);
                        sb.append(",");
                    }
                    
                    int days = (int) ((shipdate.getTime() - orderdate.getTime()) / (1000*3600*24));
                    sb.append(days);
                    sb.append(",");
                }
                catch (ParseException ex) {
                    continue;
                }
                sb.append(Float.valueOf(items[13])/Float.valueOf(items[11]));
                sb.append("\n");
                count++;
                if( count%10000 == 0 ) {
                    bw.write(sb.toString());
                    sb.setLength(0);
                }
                
            }
            bw.write(sb.toString());
            bw.close();
            long t2 = System.currentTimeMillis();
            
            m_logger.log("parse content use time:" + (t2-t1));
        }
        catch (IOException e) {
            m_logger.log("Service1 ERROR:" + e.toString());
            e.printStackTrace();
            return r;
        }
        
//        int count = 0;
//        String line = "";
//        StringBuilder sb = new StringBuilder();
//        long t1 = System.currentTimeMillis();
//        SimpleDateFormat fdateFrmat = new SimpleDateFormat("MM/dd/yyyy");
//        m_logger.log("4");
//        while (true) {
//            line = String.valueOf(Math.random());
//            if( count==0 ) {
//                count++;
//                continue;
//            }
//            if (line == null) break;
//            line=line.replace("\'", "\'\'");
//            String[] items = line.split("2");
//            if( orderSet.contains(items[0]) ) {
//                continue;
//            }
//            orderSet.add(items[0]);
//            
//            for(int i=0;i<4;++i) {
//                sb.append("\'"+items[0]);
//                sb.append("\',");
//            }
//            if( items[0].equals("L") )
//                sb.append("\'Low\'");
//            else if( items[0].equals("M") )
//                sb.append("\'Medium\'");
//            else if( items[0].equals("H") )
//                sb.append("\'High\'");
//            else if( items[0].equals("C") )
//                sb.append("\'Critical\'");
//            sb.append(",");
//            
//            
////            sb.append(Float.valueOf(items[0])/Float.valueOf(items[0]));
//            sb.append("\n");
//            count++;
////            m_logger.log("count:" + count);
//            if( (count%1000)==0 ) {
//                long now = System.currentTimeMillis();
//                if( (now-t1)>=20000 ) {
//                    m_logger.log("count:" + count);
//                    break;
//                }
//                long a2 = (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/(1024);
//                long freeMem = Runtime.getRuntime().freeMemory() / (1024*1024);
//                double freeRate = ((double)Runtime.getRuntime().freeMemory()) / (Runtime.getRuntime().totalMemory());
//                if( freeMem < 1 || freeRate<0.02 ) {
//                    m_logger.log("total:" + Runtime.getRuntime().totalMemory());
//                    m_logger.log("free:" + Runtime.getRuntime().freeMemory());
//                    m_logger.log("freeRate:" + freeRate);
//                    m_logger.log("count:" + count);
//                    break;
//                }
//            }
//            if( (count%10000)==0 ) 
//                m_logger.log("count:" + count);
//        }
        long t2 = System.currentTimeMillis();
        if( (t2-t1) <=0 )
            r.setThroughput("-1");
        else
            r.setThroughput(String.valueOf(count*1000/(t2-t1)));
        
        
        return r;
    }
    
    public static boolean setCurrentDirectory(String directory_name)
    {
        boolean result = false;  // Boolean indicating whether directory was set
        File    directory;       // Desired current working directory

        directory = new File(directory_name).getAbsoluteFile();
        if (directory.exists() || directory.mkdirs())
        {
            result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
        }

        return result;
    }
}
