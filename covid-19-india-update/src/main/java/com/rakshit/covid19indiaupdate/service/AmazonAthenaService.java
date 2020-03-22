package com.rakshit.covid19indiaupdate.service;

import com.rakshit.covid19indiaupdate.models.IamUserCredentials;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.util.Arrays;
import java.util.List;

import static com.rakshit.covid19indiaupdate.util.FileUtil.EMPTY_STRING;

@Slf4j
public class AmazonAthenaService {

    private final AthenaClient athenaClient;

    private long waitingTime;

    String outputS3FolderPath;

    public AmazonAthenaService(IamUserCredentials iamUserCredentials) {
        AwsCredentials credentials = AwsBasicCredentials.create(
                iamUserCredentials.getAccessKey(),
                iamUserCredentials.getSecretKey());
        athenaClient = AthenaClient.builder()
                .region(Region.of(iamUserCredentials.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();
        waitingTime = 0L;
        outputS3FolderPath = EMPTY_STRING;
        log.info("AWS Athena Connection Established");
    }

    public void destroy() {
        athenaClient.close();
        log.info("AWS Athena Connection Closed");
    }

    public void setOutputS3FolderPath(String outputS3FolderPath) {
        this.outputS3FolderPath = outputS3FolderPath;
    }

    public void setWaitingTime(long waitingTime) {
        this.waitingTime = waitingTime;
    }

    public void dropTable(String database, String tableName) {
        String query = String.format("DROP TABLE %s;", tableName);
        log.info("===================================");
        processQuery(database, query);
        log.info("Table Dropped : {}", tableName);
        log.info("===================================");
    }

    public void dropTableIfExists(String database, String tableName) {
        String query = String.format("DROP TABLE IF EXISTS %s;", tableName);
        log.info("===================================");
        processQuery(database, query);
        log.info("Table Dropped : {}", tableName);
        log.info("===================================");
    }

    public void dropView(String database, String viewName) {
        String query = String.format("DROP VIEW %s;", viewName);
        log.info("===================================");
        processQuery(database, query);
        log.info("View Dropped : {}", viewName);
        log.info("===================================");
    }

    public void dropViewIfExists(String database, String viewName) {
        String query = String.format("DROP VIEW IF EXISTS %s;", viewName);
        log.info("===================================");
        processQuery(database, query);
        log.info("View Dropped : {}", viewName);
        log.info("===================================");
    }

    public void loadPartitions(String database, String table) {
        String query = String.format("MSCK REPAIR TABLE %s;", table);
        log.info("===================================");
        processQuery(database, query);
        log.info("===================================");
    }

    public void view(String database, String name) {
        String query = String.format("SELECT * FROM %s;", name);
        log.info("===================================");
        processQuery(database, query);
        log.info("===================================");
    }

    public void view(String database, String name, int limit) {
        String query = String.format("SELECT * FROM %s limit %d;", name, limit);
        log.info("===================================");
        processQuery(database, query);
        log.info("===================================");
    }

    public void processQuery(String database, String query) {
        validate();
        String queryExecutionId = submitAthenaQuery(outputS3FolderPath, database, query);
        waitForQueryToComplete(queryExecutionId, waitingTime);
        processResultRows(queryExecutionId);
    }

    private void validate() {
        if (0 == waitingTime) {
            throw new RuntimeException("Set Waiting Time");
        } else if (EMPTY_STRING.equals(outputS3FolderPath)) {
            throw new RuntimeException("Set Output S3 Folder Path");
        }
    }

    public String submitAthenaQuery(String athenaOutputS3FolderPath, String database, String query) {
        QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                .database(database)
                .build();
        ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                .outputLocation(athenaOutputS3FolderPath)
                .build();
        StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(queryExecutionContext)
                .resultConfiguration(resultConfiguration)
                .build();
        StartQueryExecutionResponse startQueryExecutionResponse = athenaClient
                .startQueryExecution(startQueryExecutionRequest);
        return startQueryExecutionResponse.queryExecutionId();
    }

    public void waitForQueryToComplete(String queryExecutionId, long sleepAmountInMs) {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId)
                .build();
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            GetQueryExecutionResponse getQueryExecutionResponse = athenaClient
                    .getQueryExecution(getQueryExecutionRequest);
            QueryExecutionStatus status = getQueryExecutionResponse
                    .queryExecution()
                    .status();
            QueryExecutionState queryState = status.state();
            switch (queryState) {
                case FAILED:
                    throw new RuntimeException("Query Failed to run with Error Message: " + status.stateChangeReason());
                case CANCELLED:
                    throw new RuntimeException("Query was cancelled.");
                case SUCCEEDED:
                    isQueryStillRunning = false;
                    break;
                default:
                    try {
                        Thread.sleep(sleepAmountInMs);
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("%s%s", e.getMessage(), Arrays.toString(e.getStackTrace())));
                    }
            }
            log.info("Current Status is: {}", queryState);
        }
    }

    public void processResultRows(String queryExecutionId) {
        GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                .queryExecutionId(queryExecutionId)
                .build();
        GetQueryResultsIterable getQueryResultsResults = athenaClient.
                getQueryResultsPaginator(getQueryResultsRequest);
        for (GetQueryResultsResponse resultResult : getQueryResultsResults) {
            List<Row> rows = resultResult.resultSet().rows();
            processRow(rows);
        }
    }

    public void processRow(List<Row> rowList) {
        for (Row row : rowList) {
            for (Datum datum : row.data()) {
                log.info("{}", datum.varCharValue());
            }
        }
    }

    public void createTable(String database, String table, String metaData) {
        String query = String.format("CREATE EXTERNAL TABLE %s%s", table, metaData);
        createTable(database, query);
    }

    public void createTableIfNotExists(String database, String table, String metaData) {
        String query = String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s%s", table, metaData);
        createTable(database, query);
    }

    public void createTable(String database, String query) {
        log.info("===================================");
        processQuery(database, query);
        log.info("Table Created");
        log.info("===================================");
    }

    public void createView(String database, String view, String metaData) {
        String query = String.format("CREATE VIEW %s AS %s", view, metaData);
        createView(database, query);
    }

    public void createOrReplaceView(String database, String view, String metaData) {
        String query = String.format("CREATE OR REPLACE VIEW %s AS %s", view, metaData);
        createView(database, query);
    }

    public void createView(String database, String query) {
        log.info("===================================");
        processQuery(database, query);
        log.info("View Created");
        log.info("===================================");
    }
}
