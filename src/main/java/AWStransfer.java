import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.LocalDateTime;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class AWStransfer {

    public static void main(String[] args) {
        
    	String accessKey = "";
    	String secretKey = "";
    	String bucketName = "my-s3-bucket-sakethtest4";
    	
    	
    	try {
    		EnvLoader.loadEnv(".env");
    		accessKey = System.getProperty("AWS_ACCESS_KEY_ID");
    		secretKey = System.getProperty("AWS_SECRET_ACCESS_KEY");
    	}
    	catch(IOException e) {
    		e.printStackTrace();  
    	}
    	
    	S3Service s3Service = new S3Service(accessKey, secretKey);
    	
    	//s3Service.createBucketIfNotExists(bucketName);
    	
    	//s3Service.deleteBucket("my-s3-bucket-sakethtest6");
    	//s3Service.bucketsListInAccount();

    	//String keyName = "Test-file-"+LocalDateTime.now()+".txt"; //The file name you want in your S3 bucket
    	String keyName = "Test-file2.txt";
    	String filePath = "s3UploadFileTest.txt"; //The path where your file is located
    	//String filePath = "ibrd_loans_and_guarantees_historical_data_12-10-2024.csv";
    	s3Service.uploadObjectsToBucket(bucketName,keyName, filePath);
    	//s3Service.listNumberOfObjectsInBucket(bucketName);
    	//String downloadPath = "FileFromS3Downloaded.txt"; //The path where you want to download your file from S3 bucket
    	//s3Service.downloadFileFromBucket(bucketName, keyName, downloadPath);
    	String destBucketName = "my-s3-bucket-sakethtest3";
    	String destKeyName = "copiedFileFromOtherBucket.txt";
    	//s3Service.copyObjectInBucket(bucketName, keyName, destBucketName, destKeyName);
    	//s3Service.deleteObjectFromBucket(keyName, bucketName);
    	//List<String> keyNamestoDelete = Arrays.asList(keyName, "Test-file2.txt");
    	//s3Service.deleteMultipleObjectsFromBucket(bucketName, keyNamestoDelete);
    	
    }
    
    
}

