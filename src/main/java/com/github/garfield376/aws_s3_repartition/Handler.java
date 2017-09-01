package com.github.garfield376.aws_s3_repartition;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.AmazonClientException;
import java.lang.System;


public class Handler 
{
	private AmazonS3 s3;
	private String destinationBucketName = System.getenv("dest_bucket");   //"dest_bucket" is one environment key set at Lambda runtime

	public Handler() {
		s3 = AmazonS3ClientBuilder
		          .standard()
		          .withRegion(System.getenv("bucket_region"))  //"bucket_region" is one environment key set at Lambda runtime
		          .build();


	}
	
	public void handleRequest(S3Event event, Context context) {
		S3Entity e = event.getRecords().get(0).getS3();

		
	    String sourceBucketName = e.getBucket().getName();  // cfsrc
	    String sourceKey = e.getObject().getKey();          // prefix/EP4CMY7FDJR6Y.2017-06-28-16.ea6de06c.gz
	   
	    
	    //
	    // Rename as follows:
	    //
	    //   bucket:  cfsrc                                          -> cfdest
	    //   key:     prefix/EP4CMY7FDJR6Y.2017-06-28-16.ea6de06c.gz -> yyyy=2017/mm=06/dd=28/hh=16/EP4CMY7FDJR6Y.2017-06-28-16.ea6de06c.gz
	    //
	    
	    String regex   = "(?<distributionid>.*)\\.(?<year>[0-9]{4})\\-(?<month>[0-9]{2})\\-(?<day>[0-9]{2})\\-(?<hour>[0-9]{2})\\.(?<hash>.*)\\.gz";
	    String replace = "yyyy=${year}/mm=${month}/dd=${day}/hh=${hour}/${distributionid}.${year}-${month}-${day}-${hour}.${hash}.gz";
	    String destinationKey = sourceKey.replaceAll(regex, replace);

	    try {
	      s3.copyObject(sourceBucketName, sourceKey, 
	        destinationBucketName, destinationKey);
	    } catch (AmazonClientException ex) {
	      System.out.println(ex.toString());
	    }
	}
}
