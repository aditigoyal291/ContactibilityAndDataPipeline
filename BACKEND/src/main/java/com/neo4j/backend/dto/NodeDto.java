package com.neo4j.backend.dto;


import lombok.Data;
import java.util.Map;

@Data
public class NodeDto {
    private String id;
    private String label;
    private Map<String, Object> properties;
    private Double x;
    private Double y;
    private String type;
}
