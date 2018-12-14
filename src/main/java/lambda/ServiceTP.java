/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.fasterxml.jackson.core.JsonParser;
import faasinspector.register;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
//import org.json.simple.JSONObject;
import org.json.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

/**
 *
 * @author frank
 */
public class ServiceTP {
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
        
        setCurrentDirectory("/tmp");
//        deleteDir("/tmp/");
        m_logger = context.getLogger();
        m_s3Client = AmazonS3ClientBuilder.standard().build();
    }
    // Lambda Function Handler
    public Response handleRequest(Request request, Context context) {
        register reg = new register(m_logger);
        Response r = reg.StampContainer();
        Init(request, context);
        String flag = request.getFlag();
        if( flag.equals("1") ) {
            boolean b = Service1();
            r.setValue(b?"true":"false");
        }
        else if( flag.equals("2") ) {
            boolean b = Service2();
            r.setValue(b?"true":"false");
        }
        else if( flag.equals("3") ) {
            String res = Service3();
            r.setValue(res);
        }
        else{
            r.setValue("false");
        }
        
        return r;
    }
    
    public Response handleRequest1(Request request, Context context) {
        register reg = new register(m_logger);
        Response r = reg.StampContainer();
        Init(request, context);
        boolean b = Service1();
        r.setValue(b?"true":"false");
        return r;
    }
    
    public Response handleRequest2(Request request, Context context) {
        register reg = new register(m_logger);
        Response r = reg.StampContainer();
        Init(request, context);
        boolean b = Service2();
        r.setValue(b?"true":"false");
        return r;
    }
    
    public Response handleRequest3(Request request, Context context) {
        register reg = new register(m_logger);
        Response r = reg.StampContainer();
        Init(request, context);
        String res = Service3();
        r.setValue(res);
        return r;
    }
    
    
    boolean Service1() {
        StringBuilder res = new StringBuilder();
        
        m_logger.log("###: m_bucketName:"+m_bucketName+"   m_fileName:"+m_fileName);
        
        S3Object s3Object = null;
        try {
            s3Object = m_s3Client.getObject(new GetObjectRequest(m_bucketName, m_fileName));
        }
        catch (SdkClientException e) {
        }
        
        if( s3Object == null ) {
            m_logger.log("s3Object is null");
            return false;
        }
        else {
            long file_size = s3Object.getObjectMetadata().getContentLength();
            m_logger.log("s3Object is not null, size:"+file_size);
        }
        InputStream objectData = s3Object.getObjectContent();
        
        HashSet<String> orderSet = new HashSet();
        BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
        
        
        int count = 0;
        String line = "";
        StringBuilder sb = new StringBuilder();
        try
        {
            long t1 = System.currentTimeMillis();
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
            return false;
        }
        
        UploadSFile2S3(m_bucketNameMid, m_fileNameMid, m_fileNameLocal);
        return true;
    }
    
    boolean Service2() {
        m_logger.log("    @@@    bucketname:" + m_bucketName + "    filename:"+m_fileName);
        File file = new File(m_localFullPath);
        if( file.exists() ) {
            m_logger.log("    @@@    file exists. name:" + m_localFullPath);
            file.delete();
        }
        else {
            m_logger.log("    @@@    file NOT!! exists. create it:" + m_localFullPath);
        }
        
        S3Object s3Object = null;
        try {
            s3Object = m_s3Client.getObject(new GetObjectRequest(m_bucketNameMid, m_fileNameMid));
        }
        catch (SdkClientException e) {}
        
        if( s3Object == null ) {
            m_logger.log(m_bucketNameMid + "/" + m_fileNameMid + "do not exist." );
            return false;
        }
        else {
            long file_size = s3Object.getObjectMetadata().getContentLength();
            m_logger.log(m_bucketNameMid + "/" + m_fileNameMid + ", file size:"+file_size);
        }
        InputStream objectData = s3Object.getObjectContent();
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
        
        int count = 0;
        String line = "";
        String ins = "";
        try
        {
            Connection con = DriverManager.getConnection(m_jdbcName);
            
            // Detect if the table 'mytable' exists in the database
            m_logger.log("trying to create table 'mytable'");
            String tbs =  "CREATE TABLE mytable ( Region text, "
                        + "Country text, ItemType text, "
                        + "SalesChannel text, OrderPriority text, "
                        + "OrderDate date, OrderID integer PRIMARY KEY, "
                        + "ShipDate date, UnitsSold integer, "
                        + "UnitPrice float, UnitCost float, "
                        + "TotalRevenue float, TotalCost float, "
                        + "TotalProfit float, "
                        + "OrderProcessingTime integer, "
                        + "GrossMargin float);";
            
            PreparedStatement ps = con.prepareStatement(tbs);
            ps.execute();
            
            m_logger.log("    ### tbs: "  + tbs);
            
            ps = con.prepareStatement("PRAGMA synchronous = OFF;");
            ps.execute();
            ps.close();
            
            ps = con.prepareStatement("begin");
            ps.execute();
            ps.close();
            while (true) {
                line = reader.readLine();
                if (line == null) break;
                
                ins = "insert into mytable values(" + line + ");";
                
                if( count % 100000 == 0 )
                    m_logger.log("    @@@ index: "  + count);

                ps = con.prepareStatement(ins);
                ps.execute();
                ps.close();

                count++;
            }
            ps = con.prepareStatement("commit");
            ps.execute();
            ps.close();
            
            m_logger.log("    @@@ line count: "  + count);
            con.close();
            
        }
        catch (IOException | SQLException sqle) {
            m_logger.log("Service2 ERROR:" + sqle.toString());
            sqle.printStackTrace();
            m_logger.log("ins:" + ins);
            return false;
        }
        UploadSFile2S3(m_bucketNameDB, m_fileNameDB, m_localFullPath);
        return true;
    }
    
    String Service3() {
//        JSONObject obj = new JSONObject("{dsf}");
//        m_logger.log("    @@@ json:"  + m_aggregation);
        long t1 = System.currentTimeMillis();
        amazonS3Downloading(m_bucketNameDB, m_fileNameDB, m_localFullPath);
        long t2 = System.currentTimeMillis();
        m_logger.log("amazonS3Downloading use time:" + (t2-t1));
        
        String[] items = m_aggregation.split(",");
        int itemNum = items.length;
        boolean[] hasPercent = new boolean[itemNum];
        boolean[] hasInDays = new boolean[itemNum];
        
        String agg = "";
        for(int i=0;i<itemNum;++i) {
            hasPercent[i] = (items[i].indexOf("in percent") > 0);
            hasInDays[i] = (items[i].indexOf("in days") > 0);
            items[i] = items[i].replace("in percent", "");
            items[i] = items[i].replace("in days", "");
            agg = agg + items[i];
            if (i != itemNum-1)
                agg = agg + ", ";
        }
        
        String ql = "select " + agg + " from mytable where " + m_filter+";";
        m_logger.log("### ql:" + ql);
        StringBuilder sb = new StringBuilder();
        
        try
        {
            Connection con = DriverManager.getConnection(m_jdbcName);
            
            PreparedStatement ps = con.prepareStatement(ql);
            ResultSet rs = ps.executeQuery();
            LinkedList<String> ll = new LinkedList<String>();
            int index=1;
            
            if (rs.next())
            {
                while( index<=itemNum ) {
                    String ori = rs.getString(index);
                    String cur = ori;
                    if( hasPercent[index-1] ) {
                        double v = Double.valueOf(ori);
                        DecimalFormat df = new DecimalFormat("#.00");
                        cur = df.format(v*100)+"%";
                    }
                    else if( hasInDays[index-1] ) {
                        double v = Double.valueOf(ori);
                        cur = String.valueOf(Integer.valueOf((int)v));
                    }
                    sb.append(cur);
                    if( index < itemNum )
                        sb.append(", ");
                    index++;
                }
            }
            rs.close();
            
            
            m_logger.log("######### sb:" + sb.toString());
        }
        catch (SQLException sqle) {
            m_logger.log("Service2 ERROR:" + sqle.toString());
            sqle.printStackTrace();
            return "";
        }
        
        return sb.toString();
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

    
    void amazonS3Downloading(String bucketName,String key,String targetFilePath){
        File file=new File(targetFilePath);
        if( !file.exists() )
            m_s3Client.getObject(new GetObjectRequest(bucketName,key), file);
    }
    
    void UploadSFile2S3(String bucketname, String filename, String localFullPath) {
        long t1 = System.currentTimeMillis();
        File file = new File(localFullPath);
        if( file.exists() ) {
            m_logger.log("    @@@    db file exists. name:" + localFullPath );
        }
        else {
            m_logger.log("    @@@    db file NOT!! exists. create it:" + localFullPath );
        }
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(bucketname, filename, file);
        long t2 = System.currentTimeMillis();
        m_logger.log("######### upload time:" + (t2-t1)/1000 + "   file:"+localFullPath);
    }

    void UploadSB2S3(String bucketname, String filename, StringBuilder sw) {
        byte[] bytes = sw.toString().getBytes();//.getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(bucketname, filename, is, meta);
    }
    
    boolean deleteDir(String path){
        File file = new File(path);
        if(!file.exists()){
            return false;
        }
        String[] content = file.list();
        for(String name : content){
            File temp = new File(path, name);
            if(temp.isDirectory())
                deleteDir(temp.getAbsolutePath());
            temp.delete();
        }
        return true;
    }

    
}
