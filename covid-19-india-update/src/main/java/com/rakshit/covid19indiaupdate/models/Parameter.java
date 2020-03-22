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
public class Parameter {
    @Parsed
    private String key;
    @Parsed
    private String value;
}
