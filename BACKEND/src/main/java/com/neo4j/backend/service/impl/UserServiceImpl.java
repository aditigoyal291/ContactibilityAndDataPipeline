package com.neo4j.backend.service.impl;

import com.neo4j.backend.dao.impl.UserDaoImpl;
import com.neo4j.backend.dto.UserRequest;
import com.neo4j.backend.dto.UserResponse;
import com.neo4j.backend.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    private final UserDaoImpl userDaoImpl;


    @Override
    public UserResponse signup(UserRequest request) {
        return userDaoImpl.signup(request);
    }


    @Override
    public UserResponse login(UserRequest request) {
        log.info("before the login request in service impl");
        return userDaoImpl.login(request);

    }

    @Override
    public UserResponse addTask(String email, String task) {
        return userDaoImpl.addTask(email, task);
    }


}
