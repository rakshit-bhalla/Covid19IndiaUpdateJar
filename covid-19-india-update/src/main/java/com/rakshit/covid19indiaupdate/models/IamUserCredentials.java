package com.rakshit.covid19indiaupdate.models;

import com.univocity.parsers.annotations.Parsed;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IamUserCredentials {
    @Parsed
    private String accessKey;
    @Parsed
    private String secretKey;
    @Parsed
    private String region;
}
