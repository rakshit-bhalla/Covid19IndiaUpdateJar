package com.rakshit.covid19indiaupdate.util;

import com.google.common.base.Strings;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;
import com.univocity.parsers.common.processor.BeanListProcessor;
import com.univocity.parsers.common.processor.BeanWriterProcessor;
import com.univocity.parsers.common.processor.ObjectRowWriterProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import lombok.experimental.UtilityClass;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@UtilityClass
public final class FileUtil {
    private static final Logger log = LoggerFactory.getLogger(FileUtil.class);
    public static final String BUCKET_NAME = "bucketName";
    public static final String KEY_NAME = "keyName";
    public static final String S3_PATH = "S3Path";
    public static final String DIRECTORY = "directory";
    public static final String URL = "url";
    public static final String EMPTY_STRING = "";
    public static final String SLASH = "/";
    public static final DateTimeFormatter AWS_FILE_NAME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
    private static final DateTimeFormatter AWS_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    public static final DateTimeFormatter DATE_SLASH_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    public static final DateTimeFormatter TIME_12_HOUR_FORMATTER = DateTimeFormatter.ofPattern("hh:mm a");
    public static final String DOT_REGEX = "\\.";
    public static final String SLASH_REGEX = "\\/";
    public static final String AT = " at ";
    private static final String COMMA = ",";
    public static final String LINE_SEPARATOR = "===================================";

    public static <T> TsvWriter getTsvWriter(Class<T> model, File file, String[] headers) {
        TsvWriterSettings settings = new TsvWriterSettings();
        settings.setNullValue(EMPTY_STRING);
        settings.getFormat();
        if (0 != headers.length) {
            settings.setMaxColumns(headers.length);
            settings.setHeaderWritingEnabled(true);
            ObjectRowWriterProcessor processor = new ObjectRowWriterProcessor();
            settings.setRowWriterProcessor(processor);
            settings.setHeaders(headers);
            return new TsvWriter(file, settings);
        } else {
            settings.setRowWriterProcessor(new BeanWriterProcessor(model));
            TsvWriter writer = new TsvWriter(file, settings);
            writer.writeHeaders();
            return writer;
        }
    }

    public static <T> List<T> getBeansFromTsv(Class<T> model, InputStream inputStream) {
        TsvParserSettings settings = new TsvParserSettings();
        BeanListProcessor<T> rowProcessor = new BeanListProcessor(model);
        settings.setProcessor(rowProcessor);
        settings.setHeaderExtractionEnabled(true);
        TsvParser parser = new TsvParser(settings);
        parser.parse(inputStream);
        return rowProcessor.getBeans();
    }

    public static String getFileName(String directory, String key) {
        return String.format("%s%s%s", directory, SLASH, key);
    }

    public static String getKey(String s3Path, String partition, String keyName) {
        return String.format("%s%sdt=%s%s%s", s3Path, SLASH, partition, SLASH, keyName);
    }

    public static String[] getHeaders(Elements rowsWithHeader, int skip, List<String> extraHeaders) {
        Stream<String> dynamicHeaders = getTextValuesByElementsByTag(rowsWithHeader.first(), skip, "th");
        Stream<String> staticHeaders = extraHeaders.stream();
        return Stream.concat(dynamicHeaders, staticHeaders).toArray(String[]::new);
    }

    public static Object[] getEachRowValues(Element element, int skip, List<String> extraValues) {
        Stream<String> dynamicValues = getTextValuesByElementsByTag(element, skip, "td");
        Stream<String> staticValues = extraValues.stream();
        return Stream.concat(dynamicValues, staticValues).toArray();
    }

    public static Stream<String> getTextValuesByElementsByTag(Element element, int skip, String tag) {
        return element.getElementsByTag(tag)
                .stream()
                .skip(skip)
                .map(Element::text);
    }

    public static String getLastUpdatedTimeFromHtmlForIndia(String htmlString) {
        String string = "including foreign nationals, as on ";
        int index = htmlString.indexOf(string) + string.length();
        String[] dateAndTime = htmlString.substring(index, index + 22).split(AT);
        LocalDate date = LocalDate.parse(dateAndTime[0].replaceAll(DOT_REGEX, SLASH_REGEX),
                DATE_SLASH_FORMATTER);
        LocalTime time = LocalTime.parse(dateAndTime[1], TIME_12_HOUR_FORMATTER);
        LocalDateTime localDateTime = LocalDateTime.of(date, time);
        return localDateTime.format(AWS_DATE_TIME_FORMATTER);
    }

    public static String getStringResponseFromUrl(OkHttpClient client, String url) {
        try {
            Request request = new Request.Builder().url(url).get().build();
            Response response = client.newCall(request).execute();
            ResponseBody responseBody = response.body();
            return null != responseBody && 0 != responseBody.contentLength() ? responseBody.string() : EMPTY_STRING;
        } catch (Exception e) {
            log.info("{} {}", e.getMessage(), e.getStackTrace());
            return EMPTY_STRING;
        }
    }

    public static boolean isValidParameter(Map<String, String> parameters, String... keys) {
        String logString = Arrays.stream(keys)
                .filter(s -> Strings.isNullOrEmpty(parameters.get(s)))
                .collect(Collectors.joining(COMMA));
        boolean valid = Strings.isNullOrEmpty(logString);
        if (!valid) {
            log.error("Add {} values into paramsFile separated by TAB to run it", logString);
        }
        return valid;
    }

}
