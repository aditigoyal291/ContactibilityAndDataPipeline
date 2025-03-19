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

@Node("Person")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    @Id
    private String id;

    @Property("pan")
    private String pan;

    @Property("name")
    private String name;

    @Relationship(type = "WORKS_IN", direction = Relationship.Direction.OUTGOING)
    private Company company;

    @Relationship(type = "HAS_LOAN", direction = Relationship.Direction.OUTGOING)
    private Set<Loan> loans = new HashSet<>();

    // Used for relationships where this person is a co-applicant or reference
    @Relationship(type = "HAS_COAPPLICANT", direction = Relationship.Direction.INCOMING)
    private Set<Loan> coApplicantLoans = new HashSet<>();

    @Relationship(type = "HAS_REFERENCE", direction = Relationship.Direction.INCOMING)
    private Set<Loan> referenceLoans = new HashSet<>();
}
