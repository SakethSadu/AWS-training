
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
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
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;

import com.amazonaws.services.s3.model.Bucket;

public class S3Service {
	
	private static final Logger LOG = LoggerFactory.getLogger(S3Service.class);
	private AmazonS3 s3client;
	
	//To create a client service with the accesskey and secret key to handle all our operations
	public S3Service(String accessKey, String secretKey) {
		
    	AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
    	
    	this.s3client = AmazonS3ClientBuilder
    			.standard()
    			.withCredentials(new AWSStaticCredentialsProvider(credentials))
    			.withRegion(Regions.US_EAST_2)
    			.build();
    	
    }
	
	public void createBucketIfNotExists(String bucketName) {		
		
		if(verifyBucketExists(bucketName)) {
			LOG.info("Bucket '{}' already exists", bucketName);
		}
		else {
			LOG.info("Creating new Bucket '{}",bucketName);
			CreateBucketRequest bucketRequest = new CreateBucketRequest(bucketName);
			s3client.createBucket(bucketRequest);
			LOG.info("Bucket '{}' created successfully",bucketName);
		}
	}

		
	//Display all the existing buckets in our S3.
	public void bucketsListInAccount() {
		List<Bucket> buckets = s3client.listBuckets();
		for(Bucket bucket: buckets) {
			System.out.println(bucket.getName());
		}		
	}
	
	//Deleting a specific bucket with bucketName
	public void deleteBucket(String bucketName) {
		//Checking if bucket exists to delete or not
		if(verifyBucketExists(bucketName)) {
			LOG.info("Bucket '{}' exists and ready to delete.", bucketName);
			try {
				//if Yes - Proceeds to delete it
				DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
				s3client.deleteBucket(deleteBucketRequest);
				LOG.info("Bucket '{}' deleted Successfully",bucketName);
			}
			//Just in case to handle any other exception
			catch(S3Exception e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}
		}
		else {
			LOG.info("There is no bucket with such name : '{}' to delete.",bucketName);		}
		
	}
	
	public void uploadObjectsToBucket(String bucketName, String keyName, String filePath) {
		//Checking if bucket exists to upload or not
		if(!verifyBucketExists(bucketName)) {
			LOG.info("Bucket '{}' does not exists and file '{}' can't be uploaded.", bucketName,filePath);
		}
		else {
			try {
				//if Yes - checking if the file exists in specified location or Not.
				if(verifyFilePath(filePath)) {
					s3client.putObject(bucketName, keyName, new File(filePath));
					LOG.info("File '{}' uploaded to bucket '{}' with key '{}'.",filePath,bucketName,keyName);
				}
			}
			//Just in case - for any other exceptions.
			catch(Exception e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}
		}		
	}
	
	//To Display all the files and count in the bucket.
	public void listNumberOfObjectsInBucket(String bucketName) {
		if(verifyBucketExists(bucketName)) {
			ObjectListing objectList = s3client.listObjects(bucketName);
	        List<S3ObjectSummary> objects = objectList.getObjectSummaries();
	        LOG.info("Number of files in '{}' bucket are '{}' ",bucketName,objects.size());
	        System.out.println("Files in bucket '" + bucketName + "':");
	        for (S3ObjectSummary os : objects) {
	            System.out.println(" - " + os.getKey() + " (Size: " + os.getSize() + " bytes)");
	        }
		}
	}
	
	//To download a particular file from the bucket
	public void downloadFileFromBucket(String bucketName, String keyName, String downloadPath) {
		if(verifyBucketExists(bucketName) && verifyKey(bucketName, keyName)) {
			S3Object s3Object = s3client.getObject(bucketName, keyName);
		    S3ObjectInputStream inputStream = s3Object.getObjectContent();
		    try {
				FileUtils.copyInputStreamToFile(inputStream, new File(downloadPath));
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		    LOG.info("File '{}' downloaded from bucket '{}' to '{}'.", keyName, bucketName, downloadPath);
		}
	    
	}
	
	//To copy a file from one bucket to other bucket
	//We can also use the same method to rename the file by mentioning different key names and same bucket name.
	public void copyObjectInBucket(String srcBucketName, String srcKeyName, String destBucketName, String destKeyName ) {
		if(verifyBucketExists(srcBucketName) && verifyBucketExists(destBucketName) && verifyKey(srcBucketName, srcKeyName)) {
			s3client.copyObject(srcBucketName, srcKeyName, destBucketName, destKeyName);
			LOG.info("Successfully Copied the file '{}' from '{}' as the file '{}' to the bucket '{}'",srcKeyName, srcBucketName, destKeyName, destBucketName);
		}		
	}

	//To delete a file/Object from our bucket.
	public void deleteObjectFromBucket(String keyName, String bucketName) {
		if(verifyBucketExists(bucketName) && verifyKey(bucketName, keyName)) {
			s3client.deleteObject(bucketName, keyName);
			LOG.info("File '{}' Deleted Successfully from bucket '{}'",keyName, bucketName);
		}
	}
	
	//To delete more than one object at once as passing them as List
	public void deleteMultipleObjectsFromBucket(String bucketName, List<String> keyNames) {
        try {
            // Creating a DeleteObjectsRequest with the list of keys (file names)
            DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName)
                    .withKeys(keyNames.toArray(new String[0]));
            
            // Deleting the files from the bucket
            DeleteObjectsResult result = s3client.deleteObjects(request);
            result.getDeletedObjects().forEach(deletedObject -> {
            	LOG.info("Successfully deleted files: {}", deletedObject.getKey());
            });
            

        } catch (MultiObjectDeleteException e) {
            // To Handle the exception in case some objects couldn't be deleted
            LOG.error("Some files couldn't be deleted. Details: {}", e.getMessage());
            e.getDeletedObjects().forEach(deletedObject -> {
                LOG.error("Deleted: {}", deletedObject.getKey());
            });
            e.getErrors().forEach(error -> {
                LOG.error("Failed to delete: {} - {}", error.getKey(), error.getMessage());
            });
        } catch (Exception e) {
            LOG.error("An error occurred while deleting files: {}", e.getMessage());
        }
    }
	
	//To check if Bucket with Bucket Name exists or not.
	//It may not be the bucket we created, it generally checks with the buckets globally.
	public boolean verifyBucketExists(String bucketName) {
		if(!s3client.doesBucketExistV2(bucketName)) {
			LOG.info("Bucket '{}' does not exists.",bucketName);
			return false;
		}
		return true;
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
		
	public boolean verifyFilePath (String filePath){
		File file = new File(filePath);
		if(!file.exists()) {
			//if there is no file to upload or different location or spelled wrong.
			LOG.info("File '{}' does not exist. Please check the file in the path specified.",filePath);
			return false;
		}
		return true;
	}
	
	//To check whether the key is available or not
	public boolean verifyKey(String bucketName, String keyName) {
	    ObjectListing objectList = s3client.listObjects(bucketName);
	    
	    while (true) {
	        List<S3ObjectSummary> objects = objectList.getObjectSummaries();
	        for (S3ObjectSummary os : objects) {
	            if (os.getKey().equals(keyName)) {
	                return true;
	            }
	        }

	        //This code to check more than default limit.(1000)
	        //So using Pagination to load other set of objects.
	        if (objectList.isTruncated()) {
	            objectList = s3client.listNextBatchOfObjects(objectList);
	        } else {
	            break;
	        }
	    }

	    LOG.info("Key Name '{}' does not exist in the bucket '{}'", keyName, bucketName);
	    return false;
	}

}
