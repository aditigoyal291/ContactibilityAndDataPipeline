package com.neo4j.backend.repository;
import com.neo4j.backend.model.Company;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.neo4j.repository.query.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public interface CompanyRepository extends Neo4jRepository<Company, String> {
    Optional<Company> findByCompanyId(String companyId);

    @Query("MATCH (c:Company) " +
            "WHERE c.id = $companyId OR c.companyId = $companyId " +
            "MATCH (p:Person)-[work:WORKS_IN]->(c) " +
            "OPTIONAL MATCH (p)-[loan:HAS_LOAN]->(l:Loan) " +
            "RETURN c, p, work, l, loan")
    List<Map<String, Object>> findCompanyNetworkById(@Param("companyId") String companyId);
}
