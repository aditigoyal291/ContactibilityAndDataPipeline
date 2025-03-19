package com.neo4j.backend.repository;

import com.neo4j.backend.model.Loan;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.neo4j.repository.query.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public interface LoanRepository extends Neo4jRepository<Loan, String> {
    Optional<Loan> findByLoanId(String loanId);

    @Query("MATCH (loan:Loan) " +
            "WHERE loan.loanId = $loanId " +
            "MATCH (primaryApplicant:Person)-[hasLoan:HAS_LOAN]->(loan) " +
            "OPTIONAL MATCH (loan)-[coAppRel:HAS_COAPPLICANT]->(coApplicant:Person) " +
            "OPTIONAL MATCH (loan)-[refRel:HAS_REFERENCE]->(reference:Person) " +
            "RETURN loan, primaryApplicant, hasLoan, coApplicant, coAppRel, reference, refRel")
    List<Map<String, Object>> findLoanNetworkById(@Param("loanId") String loanId);
}
