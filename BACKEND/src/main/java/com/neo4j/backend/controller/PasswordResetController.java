package com.neo4j.backend.controller;

import com.neo4j.backend.dto.PasswordResetRequest;
import com.neo4j.backend.dto.PasswordResetResponse;
import com.neo4j.backend.dto.PasswordUpdateRequest;
import com.neo4j.backend.service.PasswordResetService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/password")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = {"http://localhost:5174", "http://localhost:5173"})
public class PasswordResetController {

    private final PasswordResetService passwordResetService;

    @PostMapping("/forgot")
    public ResponseEntity<PasswordResetResponse> forgotPassword(@RequestBody PasswordResetRequest request) {
        log.info("Received password reset request for email: {}", request.getEmail());
        PasswordResetResponse response = passwordResetService.generateResetToken(request);

        if (!response.isSuccess()) {
            return ResponseEntity.badRequest().body(response);
        }

        return ResponseEntity.ok(response);
    }

    @PostMapping("/reset")
    public ResponseEntity<PasswordResetResponse> resetPassword(@RequestBody PasswordUpdateRequest request) {
        log.info("Received password reset completion request");
        PasswordResetResponse response = passwordResetService.resetPassword(request);

        if (!response.isSuccess()) {
            return ResponseEntity.badRequest().body(response);
        }

        return ResponseEntity.ok(response);
    }
}
