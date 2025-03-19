package com.neo4j.backend.repository;

import com.neo4j.backend.model.User;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

//this is the persistence layer
public interface UserRepository extends MongoRepository<User, String> {
    Optional<User> findByEmail(String email); // Find user by email
}
