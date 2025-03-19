package com.neo4j.backend.dto;


import lombok.Data;
import java.util.Map;

@Data
public class LinkDto {
    private String id;
    private String source;
    private String target;
    private String type;
    private Map<String, Object> properties;
}