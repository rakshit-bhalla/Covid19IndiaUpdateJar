package com.rakshit.covid19indiaupdate.service;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.rakshit.covid19indiaupdate.models.IamUserCredentials;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Optional;

@Slf4j
public class AmazonS3Service {

    private final AmazonS3 amazonS3Client;

    public AmazonS3Service(IamUserCredentials iamUserCredentials) {
        AWSCredentials credentials = new BasicAWSCredentials(
                iamUserCredentials.getAccessKey(),
                iamUserCredentials.getSecretKey());
        amazonS3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(iamUserCredentials.getRegion())
                .build();
        log.info("AWS S3 Connection Established");
    }

    public void destroy() {
        amazonS3Client.shutdown();
        log.info("AWS S3 Connection Closed");
    }

    public Optional<PutObjectResult> putObject(String bucketName, String key, File file) {
        PutObjectResult putObjectResult = amazonS3Client.putObject(bucketName, key, file);
        log.info("Object {} Added to Bucket {}", key, bucketName);
        return Optional.ofNullable(putObjectResult);
    }
}
