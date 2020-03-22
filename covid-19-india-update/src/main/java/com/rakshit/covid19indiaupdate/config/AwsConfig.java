package com.rakshit.covid19indiaupdate.config;

import com.rakshit.covid19indiaupdate.models.IamUserCredentials;
import com.rakshit.covid19indiaupdate.service.AmazonAthenaService;
import com.rakshit.covid19indiaupdate.service.AmazonS3Service;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;
import java.io.InputStream;

import static com.rakshit.covid19indiaupdate.util.FileUtil.getBeansFromTsv;

@Slf4j
@Configuration
public class AwsConfig {

    private IamUserCredentials iamUserCredentials;

    public AwsConfig(@Value("${credentialFile:/home/rakshit/Desktop/Personal Projects/IAM_USER_ACCESS.tsv}")
                             String credentialFile) {
        try (InputStream inputStream = new FileInputStream(credentialFile)) {
            iamUserCredentials = getBeansFromTsv(IamUserCredentials.class, inputStream).get(0);
        } catch (Exception e) {
            log.error("{} {}", e.getMessage(), e.getStackTrace());
        }
    }

    @Bean(destroyMethod = "destroy")
    public AmazonS3Service AmazonS3Connector() {
        return new AmazonS3Service(iamUserCredentials);
    }

    @Bean(destroyMethod = "destroy")
    public AmazonAthenaService AmazonAthenaConnector() {
        return new AmazonAthenaService(iamUserCredentials);
    }
}
