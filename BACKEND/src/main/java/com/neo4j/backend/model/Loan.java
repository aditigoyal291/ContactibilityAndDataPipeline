package com.neo4j.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Property;
import org.springframework.data.neo4j.core.schema.Relationship;

import java.util.HashSet;
import java.util.Set;

@Node("Loan")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Loan {
    @Id
    private String id;

    @Property("loanId")
    private String loanId;

    @Property("amount")
    private Double amount;

    @Relationship(type = "HAS_LOAN", direction = Relationship.Direction.INCOMING)
    private Person primaryApplicant;

    @Relationship(type = "HAS_COAPPLICANT", direction = Relationship.Direction.OUTGOING)
    private Set<Person> coApplicants = new HashSet<>();

    @Relationship(type = "HAS_REFERENCE", direction = Relationship.Direction.OUTGOING)
    private Set<Person> references = new HashSet<>();
}