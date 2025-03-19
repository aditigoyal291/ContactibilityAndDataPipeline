package com.neo4j.backend.controller;

import com.neo4j.backend.dto.PasswordResetRequest;
import com.neo4j.backend.dto.PasswordResetResponse;
import com.neo4j.backend.dto.PasswordUpdateRequest;
import com.neo4j.backend.service.PasswordResetService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/auth-password")
@RequiredArgsConstructor
@Slf4j
public class AuthController {

    private final PasswordResetService passwordResetService;

    @PostMapping("/forgot")
    public ResponseEntity<PasswordResetResponse> forgotPassword(@RequestBody PasswordResetRequest request) {
        log.info("Received password reset request for email: {}", request.getEmail());
        return ResponseEntity.ok(passwordResetService.generateResetToken(request));
    }

    @PostMapping("/reset")
    public ResponseEntity<PasswordResetResponse> resetPassword(@RequestBody PasswordUpdateRequest request) {
        log.info("Received password reset request with token: {}", request.getToken());
        return ResponseEntity.ok(passwordResetService.resetPassword(request));
    }
}
