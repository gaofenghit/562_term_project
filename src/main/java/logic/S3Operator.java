package logic;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.File;
import java.io.InputStream;

public class S3Operator {


    public InputStream readFile(String bucketName, String objectKey) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object csvFileS3Object = s3Client.getObject(new GetObjectRequest(bucketName, objectKey));
        return csvFileS3Object.getObjectContent();
    }

    public void uploadFile(String bucketName, String objectKey, File file) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(bucketName, objectKey, file);
    }


}
