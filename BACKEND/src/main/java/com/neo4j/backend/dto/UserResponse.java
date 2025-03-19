package com.neo4j.backend.dto;

import lombok.Data;

import java.util.List;

@Data
public class UserResponse {
    private String message;

    private String token; // Add this field

    private String email;

    private List<String> tasks;  // Add this field


    // Optional: Constructor for convenience
    public UserResponse(String message, String token) {
        this.message = message;
        this.token = token;
    }
}
