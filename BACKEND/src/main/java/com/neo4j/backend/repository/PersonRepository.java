package com.neo4j.backend.repository;

import com.neo4j.backend.model.Person;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.neo4j.repository.query.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public interface PersonRepository extends Neo4jRepository<Person, String> {
    Optional<Person> findByPan(String pan);

    @Query("MATCH (p:Person {pan: $panNumber}) " +
            "OPTIONAL MATCH primaryPath = (p)-[:HAS_LOAN]->(l1:Loan) " +
            "OPTIONAL MATCH coappPath = (l2:Loan)-[:HAS_COAPPLICANT]->(p) " +
            "OPTIONAL MATCH refPath = (l3:Loan)-[:HAS_REFERENCE]->(p) " +
            "OPTIONAL MATCH workPath = (p)-[:WORKS_IN]->(c:Company) " +
            "OPTIONAL MATCH coappRefPath = (l1)-[:HAS_REFERENCE|HAS_COAPPLICANT]->(relatedPerson:Person) " +
            "OPTIONAL MATCH relatedPersonWorkPath = (relatedPerson)-[:WORKS_IN]->(relatedCompany:Company) " +
            "OPTIONAL MATCH relatedLoansPath = (relatedPerson)-[:HAS_LOAN]->(relatedLoan:Loan) " +
            "RETURN p, primaryPath, coappPath, refPath, workPath, coappRefPath, relatedPersonWorkPath, relatedLoansPath")
    List<Map<String, Object>> findPersonNetworkByPan(@Param("panNumber") String panNumber);
}

