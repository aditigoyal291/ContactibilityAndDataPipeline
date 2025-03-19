package com.neo4j.backend.dao;


import com.neo4j.backend.dto.GraphDataDto;
import com.neo4j.backend.dto.LinkDto;
import com.neo4j.backend.dto.NodeDto;
import com.neo4j.backend.repository.CompanyRepository;
import com.neo4j.backend.repository.GraphRepository;
import com.neo4j.backend.repository.LoanRepository;
import com.neo4j.backend.repository.PersonRepository;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class GraphDao {

    @Autowired
    private PersonRepository personRepository;

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private LoanRepository loanRepository;

    @Autowired
    private GraphRepository graphRepository;

    public GraphDataDto getGraphData() {
        List<Map<String, Object>> results = graphRepository.findGraphData();
        return processGraphData(results);
    }

    public GraphDataDto getPersonNetworkByPan(String panNumber) {
        List<Map<String, Object>> results = personRepository.findPersonNetworkByPan(panNumber);
        return processGraphData(results);
    }

    public GraphDataDto getCompanyNetworkById(String companyId) {
        List<Map<String, Object>> results = companyRepository.findCompanyNetworkById(companyId);
        return processGraphData(results);
    }

    public GraphDataDto getLoanNetworkById(String loanId) {
        List<Map<String, Object>> results = loanRepository.findLoanNetworkById(loanId);
        return processGraphData(results);
    }

    private GraphDataDto processGraphData(List<Map<String, Object>> results) {
        GraphDataDto graphDataDto = new GraphDataDto();
        Map<String, NodeDto> nodeMap = new HashMap<>();

        for (Map<String, Object> record : results) {
            for (Map.Entry<String, Object> entry : record.entrySet()) {
                Object value = entry.getValue();

                if (value instanceof Node) {
                    processNode((Node) value, nodeMap, graphDataDto.getNodes());
                } else if (value instanceof Relationship) {
                    processRelationship((Relationship) value, nodeMap, graphDataDto.getLinks());
                } else if (value instanceof Path) {
                    Path path = (Path) value;
                    // Process all nodes and relationships in the path
                    for (Node node : path.nodes()) {
                        processNode(node, nodeMap, graphDataDto.getNodes());
                    }
                    for (Relationship relationship : path.relationships()) {
                        processRelationship(relationship, nodeMap, graphDataDto.getLinks());
                    }
                }
            }
        }

        return graphDataDto;
    }

    private void processNode(Node node, Map<String, NodeDto> nodeMap, List<NodeDto> nodes) {
        String nodeId = String.valueOf(node.id());

        // Skip if we've already processed this node
        if (nodeMap.containsKey(nodeId)) {
            return;
        }

        NodeDto nodeDto = new NodeDto();
        nodeDto.setId(nodeId);
        // Use the first label as the primary label
        nodeDto.setLabel(node.labels().iterator().next());

        // Process properties
        Map<String, Object> properties = new HashMap<>();
        for (String key : node.keys()) {
            Value value = node.get(key);
            properties.put(key, convertNeo4jValue(value));
        }
        nodeDto.setProperties(properties);

        // Add to collections
        nodes.add(nodeDto);
        nodeMap.put(nodeId, nodeDto);
    }

    private void processRelationship(Relationship relationship, Map<String, NodeDto> nodeMap, List<LinkDto> links) {
        LinkDto linkDto = new LinkDto();
        linkDto.setId(String.valueOf(relationship.id()));
        linkDto.setSource(String.valueOf(relationship.startNodeId()));
        linkDto.setTarget(String.valueOf(relationship.endNodeId()));
        linkDto.setType(relationship.type());

        // Process properties
        Map<String, Object> properties = new HashMap<>();
        for (String key : relationship.keys()) {
            Value value = relationship.get(key);
            properties.put(key, convertNeo4jValue(value));
        }
        linkDto.setProperties(properties);

        links.add(linkDto);
    }

    private Object convertNeo4jValue(Value value) {
        // Convert Neo4j values to Java standard types
        switch (value.type().name()) {
            case "INTEGER":
                return value.asInt();
            case "FLOAT":
                return value.asFloat();
            case "STRING":
                return value.asString();
            case "BOOLEAN":
                return value.asBoolean();
            case "NULL":
                return null;
            case "LIST":
                return value.asList(this::convertNeo4jValue);
            case "MAP":
                Map<String, Object> map = new HashMap<>();
                value.asMap((v) -> convertNeo4jValue((Value) v)).forEach(map::put);
                return map;
            default:
                return value.asObject();
        }
    }
}