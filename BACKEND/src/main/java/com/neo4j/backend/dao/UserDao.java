package com.neo4j.backend.dao;

import com.neo4j.backend.dto.UserRequest;
import com.neo4j.backend.dto.UserResponse;

public interface UserDao {

    public UserResponse signup(UserRequest request);
    public UserResponse login(UserRequest request);
    public UserResponse addTask(String email, String task);
}
