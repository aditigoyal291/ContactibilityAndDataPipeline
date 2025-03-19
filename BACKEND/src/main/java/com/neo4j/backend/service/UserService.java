package com.neo4j.backend.service;

import com.neo4j.backend.dto.UserRequest;
import com.neo4j.backend.dto.UserResponse;

public interface UserService {

    UserResponse signup(UserRequest request);
    UserResponse login(UserRequest request);
    UserResponse addTask(String email, String task);
}
