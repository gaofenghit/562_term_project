/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import faasinspector.register;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * uwt.lambda_test::handleRequest
 * @author wlloyd
 */
public class LoadData implements RequestHandler<Request, Response>
{
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");
    
    
    // Lambda Function Handler
    public Response handleRequest(Request request, Context context) {
        // Create logger
        LambdaLogger logger = context.getLogger();
        
        // Register function
        register reg = new register(logger);

        //stamp container with uuid
        Response r = reg.StampContainer();
        
        // *********************************************************************
        // Implement Lambda Function Here
        // *********************************************************************
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        
        setCurrentDirectory("/tmp");
        Connection con = null;
        
        // read the file
        HashMap<String, Integer> map = new HashMap();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        InputStream objectData = s3Object.getObjectContent();
        Scanner scanner = new Scanner(objectData);
        Scanner valueScanner = null;
        int line = -1;
        
        byte[] zippedData = null;
        try {
            zippedData = IOUtils.toByteArray(objectData);
        } catch (IOException ex) {
            Logger.getLogger(LoadData.class.getName()).log(Level.SEVERE, null, ex);
        }
        String actualChecksum = DigestUtils.md5Hex(zippedData);
        
        String mytable = actualChecksum;//use file's md5 as the table's name
        try {
            // Detect if the table 'mytable' exists in the database
            con = DriverManager.getConnection("jdbc:sqlite:mytest.db");
            PreparedStatement ps = con.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name='" + mytable + "'");
            ResultSet rs = ps.executeQuery();
            if (rs.next())
            {
                // table already exists
                rs.close();
                con.close();
                r.setValue("Bucket:" + bucketname + " filename:" + filename + " testvalue:" + actualChecksum);
                return r;
            }
        } catch (SQLException sqle) {
            logger.log("DB ERROR:" + sqle.toString());
            sqle.printStackTrace();
        }
        
        while (scanner.hasNextLine()) {
            line++;
            valueScanner = new Scanner(scanner.nextLine());
	    valueScanner.useDelimiter(",");
            String[] row = new String[16];
            int col = 0;
            while (valueScanner.hasNext()) {
                if (col>=row.length) break;
                row[col++] = valueScanner.next();
            }
            
            if (line==0)
            {
                // create table
                try {
                    // 'mytable' does not exist, and should be created
                    logger.log("trying to create table 'mytable'");

                    String columnString = "";
                    for (int i = 0; i < row.length; i++)
                    {
                        if (columnString != "")
                            columnString += ",";
                        columnString += row[i].replaceAll("\\s*", "") + " text";
                    }

                    PreparedStatement ps = con.prepareStatement("CREATE TABLE mytable (" + columnString + ");");
                    ps.execute();
                } catch (SQLException sqle) {
                    logger.log("DB ERROR:" + sqle.toString());
                    sqle.printStackTrace();
                }
            }
            else
            {
                // Insert row into mytable
                String insertString = "";
                for (int i = 0; i < row.length; i++)
                {
                    if (insertString != "")
                        insertString += ",";
                    insertString += "'"+ row[i] + "'";
                }
                
                if (con!=null)
                {
                    try {
                        PreparedStatement ps = con.prepareStatement("insert into mytable values(" + insertString + ");");
                        ps.execute();
                    } catch (SQLException sqle) {
                        logger.log("DB ERROR:" + sqle.toString());
                        sqle.printStackTrace();
                    }
                }
                
            }
            
        }
        scanner.close();
        
        String testvalue = "";
        if (con!=null)
        {
            try {
                // Query mytable to obtain full resultset
                PreparedStatement ps = con.prepareStatement("select * from mytable limit 1;");
                ResultSet rs = ps.executeQuery();
                // Load query results for [name] column into a Java Linked List
                // ignore [col2] and [col3] 
                while (rs.next())
                {
                    testvalue = rs.getString("Region");
                }
                rs.close();

                con.close();
            } catch (SQLException sqle) {
                logger.log("DB ERROR:" + sqle.toString());
                sqle.printStackTrace();
            }
            
        }
        
        // Set return result in Response class, class is marshalled into JSON
        r.setValue("Bucket:" + bucketname + " filename:" + filename + " testvalue:" + actualChecksum);
        
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
    
    // int main enables testing function from cmd line
    public static void main (String[] args)
    {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };
        
        // Create an instance of the class
        LoadData lt = new LoadData();
        
        // Create a request object
        Request req = new Request();
        
        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");
        
        // Load the name into the request object
        req.setBucketname(name);

        // Report name to stdout
        System.out.println("cmd-line param name=" + req.getBucketname());
        
        // Run the function
        Response resp = lt.handleRequest(req, c);
        
        // Print out function result
        System.out.println("function result:" + resp.toString());
    }
}
