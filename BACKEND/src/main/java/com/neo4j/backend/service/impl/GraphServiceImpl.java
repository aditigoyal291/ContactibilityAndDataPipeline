package com.neo4j.backend.service.impl;

import com.neo4j.backend.repository.CompanyRepository;
import com.neo4j.backend.repository.LoanRepository;
import com.neo4j.backend.repository.PersonRepository;
import com.neo4j.backend.dto.GraphDataDto;
import com.neo4j.backend.dto.LinkDto;
import com.neo4j.backend.dto.NodeDto;
import com.neo4j.backend.exception.ResourceNotFoundException;
import com.neo4j.backend.service.GraphService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class GraphServiceImpl implements GraphService {
    private final Neo4jClient neo4jClient;
    private final PersonRepository personRepository;
    private final CompanyRepository companyRepository;
    private final LoanRepository loanRepository;

    @Override
    public GraphDataDto getFullGraph() {
        // Convert Collection to List using new ArrayList<>()
        Collection<Map<String, Object>> resultCollection = neo4jClient.query("MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 100")
                .fetch()
                .all();

        // Convert Collection to List
        List<Map<String, Object>> results = new ArrayList<>(resultCollection);

        return processGraphData(results);
    }

    @Override
    public GraphDataDto getPersonByPan(String panNumber) {
        log.info("Fetching person with PAN: {}", panNumber);

        Collection<Map<String, Object>> resultCollection = neo4jClient.query(
                        "MATCH (p:Person {pan: $panNumber}) " +
                                "OPTIONAL MATCH primaryPath = (p)-[:HAS_LOAN]->(l1:Loan) " +
                                "OPTIONAL MATCH coappPath = (l2:Loan)-[:HAS_COAPPLICANT]->(p) " +
                                "OPTIONAL MATCH refPath = (l3:Loan)-[:HAS_REFERENCE]->(p) " +
                                "OPTIONAL MATCH workPath = (p)-[:WORKS_IN]->(c:Company) " +
                                "OPTIONAL MATCH coappRefPath = (l1)-[:HAS_REFERENCE|HAS_COAPPLICANT]->(relatedPerson:Person) " +
                                "OPTIONAL MATCH relatedPersonWorkPath = (relatedPerson)-[:WORKS_IN]->(relatedCompany:Company) " +
                                "OPTIONAL MATCH relatedLoansPath = (relatedPerson)-[:HAS_LOAN]->(relatedLoan:Loan) " +
                                "RETURN p, primaryPath, coappPath, refPath, workPath, coappRefPath, relatedPersonWorkPath, relatedLoansPath")
                .bind(panNumber).to("panNumber")
                .fetch()
                .all();

        // Convert Collection to List
        List<Map<String, Object>> results = new ArrayList<>(resultCollection);

        if (results.isEmpty()) {
            throw new ResourceNotFoundException("No person found with PAN: " + panNumber);
        }

        return processGraphData(results);
    }

    @Override
    public GraphDataDto getCompanyById(String companyId) {
        log.info("Fetching company with ID: {}", companyId);

        Collection<Map<String, Object>> resultCollection = neo4jClient.query(
                        "MATCH (c:Company) " +
                                "WHERE c.id = $companyId OR c.companyId = $companyId " +
                                "MATCH (p:Person)-[work:WORKS_IN]->(c) " +
                                "OPTIONAL MATCH (p)-[loan:HAS_LOAN]->(l:Loan) " +
                                "RETURN c, p, work, l, loan")
                .bind(companyId).to("companyId")
                .fetch()
                .all();

        // Convert Collection to List
        List<Map<String, Object>> results = new ArrayList<>(resultCollection);

        if (results.isEmpty()) {
            throw new ResourceNotFoundException("No company found with ID: " + companyId);
        }

        return processGraphData(results);
    }

    @Override
    public GraphDataDto getLoanById(String loanId) {
        log.info("Fetching loan with ID: {}", loanId);

        Collection<Map<String, Object>> resultCollection = neo4jClient.query(
                        "MATCH (loan:Loan) " +
                                "WHERE loan.loanId = $loanId " +
                                "MATCH (primaryApplicant:Person)-[hasLoan:HAS_LOAN]->(loan) " +
                                "OPTIONAL MATCH (loan)-[coAppRel:HAS_COAPPLICANT]->(coApplicant:Person) " +
                                "OPTIONAL MATCH (loan)-[refRel:HAS_REFERENCE]->(reference:Person) " +
                                "RETURN loan, primaryApplicant, hasLoan, coApplicant, coAppRel, reference, refRel")
                .bind(loanId).to("loanId")
                .fetch()
                .all();

        // Convert Collection to List
        List<Map<String, Object>> results = new ArrayList<>(resultCollection);

        if (results.isEmpty()) {
            throw new ResourceNotFoundException("No loan found with ID: " + loanId);
        }

        return processGraphData(results);
    }

    // Keep the rest of the code as in the previous version
    private GraphDataDto processGraphData(List<Map<String, Object>> results) {
        List<NodeDto> nodes = new ArrayList<>();
        List<LinkDto> links = new ArrayList<>();
        Map<Long, NodeDto> nodeMap = new HashMap<>();

        for (Map<String, Object> result : results) {
            for (Map.Entry<String, Object> entry : result.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // Skip null values
                if (value == null) {
                    continue;
                }

                // Process the value based on its type
                if (value instanceof Path) {
                    Path path = (Path) value;
                    // Process nodes and relationships in paths
                    for (Node node : path.nodes()) {
                        processNode(node, nodeMap, nodes);
                    }
                    for (Relationship relationship : path.relationships()) {
                        processRelationship(relationship, nodeMap, links);
                    }
                } else if (value instanceof Node) {
                    // Process individual nodes
                    processNode((Node) value, nodeMap, nodes);
                } else if (value instanceof Relationship) {
                    // Process individual relationships
                    processRelationship((Relationship) value, nodeMap, links);
                }
            }
        }

        return new GraphDataDto(nodes, links);
    }

    private void processNode(Node node, Map<Long, NodeDto> nodeMap, List<NodeDto> nodes) {
        long nodeId = node.id();
        if (!nodeMap.containsKey(nodeId)) {
            NodeDto processedNode = new NodeDto();
            processedNode.setId(String.valueOf(nodeId));

            // Get first label if available
            Iterator<String> labelIterator = node.labels().iterator();
            if (labelIterator.hasNext()) {
                processedNode.setLabel(labelIterator.next());
            }

            // Process properties
            Map<String, Object> properties = new HashMap<>();
            for (String key : node.keys()) {
                properties.put(key, node.get(key).asObject());
            }
            processedNode.setProperties(properties);

            nodes.add(processedNode);
            nodeMap.put(nodeId, processedNode);
        }
    }

    private void processRelationship(Relationship rel, Map<Long, NodeDto> nodeMap, List<LinkDto> links) {
        LinkDto linkDto = new LinkDto();
        linkDto.setId(String.valueOf(rel.id()));
        linkDto.setSource(String.valueOf(rel.startNodeId()));
        linkDto.setTarget(String.valueOf(rel.endNodeId()));
        linkDto.setType(rel.type());

        // Process properties
        Map<String, Object> properties = new HashMap<>();
        for (String key : rel.keys()) {
            properties.put(key, rel.get(key).asObject());
        }
        linkDto.setProperties(properties);

        links.add(linkDto);
    }
}