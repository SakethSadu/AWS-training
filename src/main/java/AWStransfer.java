import java.io.IOException;

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
    	String bucketName = "my-s3-bucket-sakethtest5";
    	
    	try {
    		EnvLoader.loadEnv(".env");
    		accessKey = System.getProperty("AWS_ACCESS_KEY_ID");
    		secretKey = System.getProperty("AWS_SECRET_ACCESS_KEY");
    	}
    	catch(IOException e) {
    		e.printStackTrace();  
    	}
    	
    	S3Service s3Service = new S3Service(accessKey, secretKey);
    	
    	s3Service.createBucketIfNotExists(bucketName);
    	
    	s3Service.deleteBucket("my-s3-bucket-sakethtest2");
    	s3Service.bucketsListInAccount();
    	
    }
    
    
}

