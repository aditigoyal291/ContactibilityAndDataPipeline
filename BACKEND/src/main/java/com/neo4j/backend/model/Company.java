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

@Node("Company")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Company {
    @Id
    private String id;

    @Property("companyId")
    private String companyId;

    @Property("name")
    private String name;

    @Relationship(type = "WORKS_IN", direction = Relationship.Direction.INCOMING)
    private Set<Person> employees = new HashSet<>();
}
