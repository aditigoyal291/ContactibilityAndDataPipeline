package com.neo4j.backend.repository;

import com.neo4j.backend.model.Graph;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.neo4j.repository.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface GraphRepository extends Neo4jRepository<Graph, String> {
    @Query("MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 100")
    List<Map<String, Object>> findGraphData();
}
