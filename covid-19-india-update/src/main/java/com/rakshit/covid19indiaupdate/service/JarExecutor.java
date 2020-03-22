package com.rakshit.covid19indiaupdate.service;

import com.rakshit.covid19indiaupdate.models.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.rakshit.covid19indiaupdate.util.FileUtil.getBeansFromTsv;

@Slf4j
@Service
public class JarExecutor {

    @Autowired
    private Covid19StateWiseService covid19StateWiseService;

    @Value("${paramsFile:/home/rakshit/Desktop/Personal Projects/PARAMETERS.tsv}")
    private String paramsFile;

    public void execute() {
        try (InputStream inputStream = new FileInputStream(paramsFile)) {
            Map<String, String> parameters = getBeansFromTsv(Parameter.class, inputStream)
                    .stream()
                    .collect(Collectors.toMap(Parameter::getKey, Parameter::getValue));
            Map<String, Object> additionalParameters = new HashMap();
            covid19StateWiseService.execute(parameters, additionalParameters);
        } catch (Exception var15) {
            log.error("{} {}", var15.getMessage(), var15.getStackTrace());
        }
    }
}
