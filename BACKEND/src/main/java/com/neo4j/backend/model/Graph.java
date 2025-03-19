package com.neo4j.backend.model;

import org.springframework.data.neo4j.core.schema.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.neo4j.core.schema.Node;

@Node("Graph")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Graph {
    @Id
    private String id;

    // Add other properties if needed
}
