
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
import com.amazonaws.services.s3.model.S3Object;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;



public class ProcessCSV implements RequestHandler<Request, Response>
{
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");
    
    // Lambda Function Handler
    @Override
    public Response handleRequest(Request request, Context context) {
        Response r = new Response();
        LambdaLogger logger = context.getLogger();

        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        logger.log("    @@@    bucketname:" + bucketname + "    filename:"+filename);
   
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        InputStream objectData = s3Object.getObjectContent();
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
        try {
            long total = 0;
            int count = 0;
            while (true) {
                String line = reader.readLine();
                if (line == null) break;
                String[] items = line.trim().split(",");
                for(String str : items) {
                    int num = Integer.valueOf(str);
//                    total += num;
                    count++;
                }
            }
            double avg = ((double)total) / count;
            logger.log("    @@@    ProcessCSV bucketname:" + bucketname + "  filename:" + filename + "  avgelement:" + avg + "  total:" + total+ "  count:"+count);
        } catch (IOException e) {
            logger.log("    @@@    Error occurs.");
        }
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
        ProcessCSV lt = new ProcessCSV();
        
        // Create a request object
        Request req = new Request();
        
        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");
        
        // Load the name into the request object

        
        // Run the function
        Response resp = lt.handleRequest(req, c);
        
        // Print out function result
        System.out.println("function result:" + resp.toString());
    }
}
