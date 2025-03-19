package com.neo4j.backend.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GraphDataDto {
    private List<NodeDto> nodes;
    private List<LinkDto> links;
}
