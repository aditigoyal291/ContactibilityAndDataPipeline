package com.neo4j.backend.model;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Document(collection = "AgentDetails")
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class User {

    @Id  //mongodb does not have functionality for auto increment like sql
    private String id;

    @NonNull
    @Indexed(unique = true)
    private String email;

    @NonNull
    private String password;

    private List<String> tasks; // Ensure tasks is initialized
}
