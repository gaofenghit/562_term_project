/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
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
import com.amazonaws.services.s3.model.PutObjectResult;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
    // Lambda Function Handler
    public Response handleRequest(Request request, Context context) {
        // Create logger
        LambdaLogger logger = context.getLogger();
        
        // Register function
        register reg = new register(logger);

        //stamp container with uuid
        Response r = reg.StampContainer();
        
        setCurrentDirectory("/tmp");
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        logger.log("    @@@    bucketname:" + bucketname + "    filename:"+filename);
        String db_file = filename+".db";
        String db_file_full_path = "/tmp/"+filename+".db";
        String jdbc_name = "jdbc:sqlite:"+filename+".db";
        File file = new File(db_file_full_path);
        if( file.exists() ) {
            logger.log("    @@@    file exists. name:" + db_file_full_path);
            file.delete();
        }
        else {
            logger.log("    @@@    file NOT!! exists. create it:" + db_file_full_path);
        }
        
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = null;
        try {
            s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        }
        catch (SdkClientException e) {
        }
        
        if( s3Object == null ) {
            logger.log("s3Object is null");
            return r;
        }
        else {
            long file_size = s3Object.getObjectMetadata().getContentLength();
            logger.log("s3Object is not null, size:"+file_size);
        }
        InputStream objectData = s3Object.getObjectContent();
        
        HashSet<String> orderSet = new HashSet();
//        ArrayList<String> lines = new ArrayList(1800000);
        BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
        
        int count = 0;
        String line = "";
        StringBuffer sb = new StringBuffer();
        String ins = "";
        try
        {
            Connection con = DriverManager.getConnection(jdbc_name); 
            
            // Detect if the table 'mytable' exists in the database
            logger.log("trying to create table 'mytable'");
            String tbs =  "CREATE TABLE mytable ( Region text, "
                        + "Country text, ItemType text, "
                        + "SalesChannel text, OrderPriority text, "
                        + "OrderDate date, OrderID integer, "
                        + "ShipDate date, UnitsSold integer, "
                        + "UnitPrice float, UnitCost float, "
                        + "TotalRevenue float, TotalCost float, "
                        + "TotalProfit float, "
                        + "OrderProcessingTime integer, "
                        + "GrossMargin float);";
            
            PreparedStatement ps = con.prepareStatement(tbs);
            ps.execute();
            
            logger.log("    ### tbs: "  + tbs);
            
            ps = con.prepareStatement("PRAGMA synchronous = OFF;");
            ps.execute();
            
            SimpleDateFormat fdateFrmat = new SimpleDateFormat("MM/dd/yyyy");
            
            ps = con.prepareStatement("begin");
            ps.execute();
            ps.close();
            while (true) {
                line = reader.readLine();
                if (line == null) break;
                line=line.replace("\'", "\'\'");
                String[] items = line.split(",");
                if( orderSet.contains(items[6]) ) {
                    continue;
                }
                orderSet.add(items[6]);
                sb.setLength(0);
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
                for(int i=5;i<14;++i) {
                    sb.append(items[i]);
                    sb.append(",");
                }
                try {
                    Date orderdate = fdateFrmat.parse(items[5]);
                    Date shipdate = fdateFrmat.parse(items[7]);
                    int days = (int) ((shipdate.getTime() - orderdate.getTime()) / (1000*3600*24));
                    sb.append(days);
                    sb.append(",");
                }
                catch (ParseException ex) {
                    continue;
                }
                sb.append(Float.valueOf(items[13])/Float.valueOf(items[11]));
                ins = "insert into mytable values(" + sb.toString() + ");";
                
                if( count % 100000 == 0 )
                    logger.log("    @@@ index: "  + count);
                
                if( count == 10 )
                    logger.log("    ### 10th ins: "  + ins);
                    

                ps = con.prepareStatement(ins);
                ps.execute();
                ps.close();

                count++;
            }
            ps = con.prepareStatement("commit");
            ps.execute();
            ps.close();

            logger.log("    @@@ line count: "  + count);
            con.close();
            
        }
        catch (IOException | SQLException sqle) {
            logger.log("count:" + count);
            logger.log("line:" + line);
            logger.log("sb:" + sb.toString());
            logger.log("@@@ ins:"  + ins);
            logger.log("DB ERROR:" + sqle.toString());
            sqle.printStackTrace();
            return r;
        }
        SaveFile2S3(db_file, logger);
        logger.log("    @@@ everything finish.");
        
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
        
    void SaveFile2S3(String db_file, LambdaLogger logger) {
        File file = new File("/tmp/"+db_file);
        if( file.exists() ) {
            logger.log("    @@@    db file exists. name:" + "/tmp/"+db_file );
        }
        else {
            logger.log("    @@@    db file NOT!! exists. create it:" + "/tmp/"+db_file );
        }
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject("fg.db.files", db_file, file);
    }
}
