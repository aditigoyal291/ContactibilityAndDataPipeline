package com.neo4j.backend.dao.impl;

import com.neo4j.backend.dao.UserDao;
import com.neo4j.backend.dto.UserRequest;
import com.neo4j.backend.dto.UserResponse;
import com.neo4j.backend.model.User;
import com.neo4j.backend.repository.UserRepository;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.security.Key;
import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserDaoImpl implements UserDao {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final Key SECRET_KEY = Keys.secretKeyFor(SignatureAlgorithm.HS256);
    UserResponse userResponse = new UserResponse("", null);

    @Override
    public UserResponse signup(UserRequest request){

        // Check if user already exists
        Optional<User> existingUser = userRepository.findByEmail(request.getEmail());
        if (existingUser.isPresent()) {
            userResponse.setMessage("User Already Exists");
            return userResponse;
        }

        // Create and save new user
        User newUser = new User();
        newUser.setEmail(request.getEmail());
        // Encode the password before saving
        newUser.setPassword(passwordEncoder.encode(request.getPassword()));
        userRepository.save(newUser);
        userResponse.setMessage("Registered Successfully");
        return userResponse;
    }

    @Override
    public UserResponse login(UserRequest request) {
        UserResponse userResponse = new UserResponse("", null);

        // Check if the user exists
        Optional<User> existingUser = userRepository.findByEmail(request.getEmail());

        if (existingUser.isPresent()) {
            User user = existingUser.get();
            // Check password using passwordEncoder.matches()
            if (passwordEncoder.matches(request.getPassword(), user.getPassword())) {
                // Generate JWT Token
                String token = generateToken(user.getEmail());

                userResponse.setMessage("Login Successful");
                userResponse.setToken(token); // Send token in response
                userResponse.setTasks(user.getTasks() != null ? user.getTasks() : new ArrayList<>()); // Ensure tasks is never null

                return userResponse;
            } else {
                userResponse.setMessage("Invalid Credentials");
                return userResponse;
            }
        }

        userResponse.setMessage("User Does not exist. Please Signup");
        return userResponse;
    }

    private String generateToken(String email) {
        return Jwts.builder()
                .setSubject(email)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + 1000 * 60 * 60)) // 1 hour expiry
                .signWith(SignatureAlgorithm.HS256, SECRET_KEY)
                .compact();
    }

    @Override
    public UserResponse addTask(String email, String task) {
        Optional<User> existingUser = userRepository.findByEmail(email);
        if (existingUser.isPresent()) {
            User user = existingUser.get();

            if (user.getTasks() == null) {
                user.setTasks(new ArrayList<>()); // Initialize if null
            }

            user.getTasks().add(task);
            userRepository.save(user); // Save updated user document

            return new UserResponse("Task Added Successfully", null);
        }

        return new UserResponse("User Not Found", null);
    }
}

