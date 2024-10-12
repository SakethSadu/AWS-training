
import java.util.List;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import com.amazonaws.services.s3.model.Bucket;

public class S3Service {
	
	private static final Logger LOG = LoggerFactory.getLogger(S3Service.class);
	private AmazonS3 s3client;
	
	
	public S3Service(String accessKey, String secretKey) {
		
    	AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
    	
    	this.s3client = AmazonS3ClientBuilder
    			.standard()
    			.withCredentials(new AWSStaticCredentialsProvider(credentials))
    			.withRegion(Regions.US_EAST_2)
    			.build();
    	
    }
	
	public void createBucketIfNotExists(String bucketName) {
		
		//This checks if bucket exists with the same number.
		//It may not be the bucket we created, it generally checks with the buckets globally.
		if(s3client.doesBucketExistV2(bucketName)) {
			LOG.info("Bucket '{}' already exists", bucketName);
		}
		else {
			LOG.info("Creating new Bucket '{}",bucketName);
			CreateBucketRequest bucketRequest = new CreateBucketRequest(bucketName);
			s3client.createBucket(bucketRequest);
			LOG.info("Bucket '{}' created successfully",bucketName);
		}
	}

	//To check the bucket with same name exists in our account.
	public boolean bucketExistsInMyAccount(String bucketName) {
		List<Bucket> buckets = s3client.listBuckets();
		for(Bucket bucket: buckets) {
			if(bucket.getName().equals(bucketName)) {
				return true;
			}
		}
		return false;
	}
	
	public void bucketsListInAccount() {
		List<Bucket> buckets = s3client.listBuckets();
		for(Bucket bucket: buckets) {
			System.out.println(bucket.getName());
		}		
	}
	
	public void deleteBucket(String bucketName) {
		try {
			DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
			s3client.deleteBucket(deleteBucketRequest);
			LOG.info("Bucket '{}' deleted Successfully",bucketName);
		}
		catch(S3Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
