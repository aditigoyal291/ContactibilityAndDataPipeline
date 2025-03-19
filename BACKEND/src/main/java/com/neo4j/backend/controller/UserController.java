package com.neo4j.backend.controller;

import com.neo4j.backend.dto.UserRequest;
import com.neo4j.backend.dto.UserResponse;
import com.neo4j.backend.service.impl.UserServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/user")
@RequiredArgsConstructor
@CrossOrigin(origins = {"http://localhost:5174", "http://localhost:5173"}) // Allow requests from frontend
public class UserController {
    private final UserServiceImpl userService;

    @PostMapping("/signup")
    public ResponseEntity<UserResponse> signup(@RequestBody UserRequest request) {
        UserResponse response = userService.signup(request);

        if ("User Already Exists".equals(response.getMessage())) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(response);
        }

        return ResponseEntity.ok(response);
    }


    @PostMapping("/login")
    public ResponseEntity<UserResponse> login(@RequestBody UserRequest request) {
        log.info("before the login request");
        UserResponse response = userService.login(request);
        log.info("after the login request");

        if ("Invalid Credentials".equals(response.getMessage())) {
            return ResponseEntity.status(401).body(response); // Unauthorized - Wrong password
        }
        if ("User Does not exist. Please Signup".equals(response.getMessage())) {
            return ResponseEntity.status(404).body(response); // Not Found - User doesn't exist
        }
        return ResponseEntity.ok(response); // Return JWT token on success
    }

    @PutMapping("/addtask")
    public ResponseEntity<UserResponse> addTask(@RequestParam String email, @RequestParam String task) {
        UserResponse response = userService.addTask(email, task);
        return ResponseEntity.ok(response);
    }

}
