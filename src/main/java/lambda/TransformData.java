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
import faasinspector.register;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * uwt.lambda_test::handleRequest
 * @author wlloyd
 */
public class TransformData implements RequestHandler<Request, Response>
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
        
        int index = filename.lastIndexOf(".");
        String prefix = filename.substring(0, index);
        String suffix = filename.substring(index + 1);
        
        String newfilename = prefix+"_processed"+"."+suffix;
        StringWriter sw = new StringWriter();
        
        // read the file
        HashMap<String, Integer> map = new HashMap();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        InputStream objectData = s3Object.getObjectContent();
        Scanner scanner = new Scanner(objectData);
        Scanner valueScanner = null;
        int line = -1;
        
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
                // header
                row[14] = "Order Processing Time";
                row[15] = "Gross Margin";
            }
            else
            {
                String orderId = row[6];
                if (map.get(orderId) != null)
                    continue;
                map.put(orderId, 1);

                // Add column [Order Processing Time]
                SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
                try {
                    Date orderdate = format.parse(row[5]);
                    Date shipdate = format.parse(row[7]);
                    int days = (int) ((shipdate.getTime() - orderdate.getTime()) / (1000*3600*24));
                    row[14] = Integer.toString(days);
                } catch (ParseException ex) {
                    Logger.getLogger(TransformData.class.getName()).log(Level.SEVERE, null, ex);
                }

                // Transform [Order Priority] column
                switch(row[4])
                {
                    case "L":
                        row[4] = "Low";
                        break;
                    case "M":
                        row[4] = "Medium";
                        break;
                    case "H":
                        row[4] = "High";
                        break;
                    case "C":
                        row[4] = "Critical";
                        break;
                }

                // Add a [Gross Margin] column
                float percent = Float.parseFloat(row[13])/Float.parseFloat(row[11]);
                row[15] = Float.toString((float)Math.round(percent*100)/100);
            }
            for (int i = 0; i < row.length; i++)
            {
                sw.append(row[i]);
                if ((i+1)!=row.length)
                    sw.append(",");
                else
                    sw.append("\n");
            }
            
        }
        scanner.close();
        
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("application/csv");
        // Create new file on S3
        s3Client.putObject(bucketname, newfilename, is, meta);
        
        // Set return result in Response class, class is marshalled into JSON
        r.setValue("Bucket:" + bucketname + " new filename:" + newfilename + " size:" + bytes.length);
        
        return r;
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
        TransformData lt = new TransformData();
        
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
