package com.rakshit.covid19indiaupdate.service;

import com.squareup.okhttp.OkHttpClient;
import com.univocity.parsers.tsv.TsvWriter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.rakshit.covid19indiaupdate.util.FileUtil.*;

@Service
public class Covid19StateWiseService {

    @Autowired
    private AmazonS3Service amazonS3Service;
    @Autowired
    private AmazonAthenaService amazonAthenaService;

    public void execute(Map<String, String> parameters, Map<String, Object> additionalParameters) {
        if (isValidParameter(parameters, BUCKET_NAME, KEY_NAME, S3_PATH, DIRECTORY, URL)) {
            String bucketName = parameters.get(BUCKET_NAME);
            String keyName = parameters.get(KEY_NAME);
            String s3Path = parameters.get(S3_PATH);
            String directory = parameters.get(DIRECTORY);
            String url = parameters.get(URL);
            OkHttpClient client = new OkHttpClient();
            String partition = LocalDateTime.now().format(AWS_FILE_NAME_FORMATTER);
            String key = getKey(s3Path, partition, keyName);
            String fileName = getFileName(directory, key);
            String htmlString = getStringResponseFromUrl(client, url);
            File file = new File(fileName);
            writeResponseToFile(htmlString, file);
            moveFileToS3(bucketName, key, file);
            updateAthenaTablesAndViews(bucketName, s3Path, partition);
        }
    }

    private void moveFileToS3(String bucketName, String key, File file) {
        amazonS3Service.putObject(bucketName, key, file);
    }

    private void writeResponseToFile(String htmlString, File file) {
        List<String> staticHeaders = new ArrayList();
        List<String> staticValues = new ArrayList();
        String lastUpdated = getLastUpdatedTimeFromHtmlForIndia(htmlString);
        staticHeaders.add("Last Updated");
        staticValues.add(lastUpdated);
        Document document = Jsoup.parse(htmlString);
        int skip = 1;
        Elements rowsWithHeader = document.getElementsByClass("table-responsive").last()
                .getElementsByTag("tr");
        String[] headers = getHeaders(rowsWithHeader, skip, staticHeaders);
        TsvWriter writer = getTsvWriter(Class.class, file, headers);
        rowsWithHeader.subList(1, rowsWithHeader.size() - 1)
                .stream()
                .map((element) -> getEachRowValues(element, skip, staticValues))
                .forEach(writer::processRecord);
        writer.close();
    }

    public void updateAthenaTablesAndViews(String bucketName, String s3Path, String partition) {
        String outputS3FolderPath = String.format("s3://%s/query_results", bucketName);
        String inputS3FolderPath = String.format("s3://%s%s%s", bucketName, "/", s3Path);

        amazonAthenaService.setWaitingTime(1000);
        amazonAthenaService.setOutputS3FolderPath(outputS3FolderPath);

        String database = "covid_19";
        String tableName = "corona_virus_indian_state_update";
        String viewName = "corona_indian_state_wise_dashboard_latest";

        String tableMetaData = "(state_or_union_territory string, indian_national_cases int, foreign_national_cases int, " +
                "recovered int, death int, last_updated timestamp) PARTITIONED BY (`dt` string) " +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' " +
                "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' " +
                "LOCATION '" + inputS3FolderPath +
                "' TBLPROPERTIES ('has_encrypted_data'='false', 'skip.header.line.count'='1'," +
                " 'transient_lastDdlTime'='1584691951')";
        String viewMetaData = "SELECT * FROM covid_19.corona_virus_indian_state_update WHERE (\"dt\" = '" + partition + "')";
        amazonAthenaService.createTableIfNotExists(database, tableName, tableMetaData);
        amazonAthenaService.loadPartitions(database, tableName);
        amazonAthenaService.createOrReplaceView(database, viewName, viewMetaData);
    }

}
