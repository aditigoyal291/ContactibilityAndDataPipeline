package com.neo4j.backend.service;

import com.neo4j.backend.dto.PasswordResetRequest;
import com.neo4j.backend.dto.PasswordResetResponse;
import com.neo4j.backend.dto.PasswordUpdateRequest;
import com.neo4j.backend.model.User;
import com.neo4j.backend.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class PasswordResetService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final EmailService emailService;

    private static final String TOKEN_PREFIX = "password_reset:";
    private static final int EXPIRATION_MINUTES = 30;
    private static final int MIN_PASSWORD_LENGTH = 8;

    public PasswordResetResponse generateResetToken(PasswordResetRequest request) {
        String email = request.getEmail().trim().toLowerCase();

        Optional<User> userOptional = userRepository.findByEmail(email);
        if (userOptional.isEmpty()) {
            log.warn("Password reset attempt for non-existent email: {}", email);
            return new PasswordResetResponse("User not found with email: " + email, false);
        }

        // Generate Reset Token
        String token = UUID.randomUUID().toString();
        String redisKey = TOKEN_PREFIX + token;

        // Store Token in Redis
        redisTemplate.opsForValue().set(redisKey, email);
        redisTemplate.expire(redisKey, EXPIRATION_MINUTES, TimeUnit.MINUTES);

        log.info("Generated password reset token for: {}", email);

        // Send Reset Email
        try {
            emailService.sendPasswordResetEmail(email, token);
            return new PasswordResetResponse("Password reset link sent to your email", true);
        } catch (Exception e) {
            log.error("Failed to send password reset email: {}", e.getMessage());
            return new PasswordResetResponse("Failed to send password reset email, please try again later", false);
        }
    }

    public PasswordResetResponse resetPassword(PasswordUpdateRequest request) {
        String token = request.getToken();
        String newPassword = request.getNewPassword().trim();

        // Validate token
        String redisKey = TOKEN_PREFIX + token;
        String email = (String) redisTemplate.opsForValue().get(redisKey);

        if (email == null) {
            log.warn("Invalid or expired password reset token: {}", token);
            return new PasswordResetResponse("Invalid or expired password reset token", false);
        }

        Optional<User> userOptional = userRepository.findByEmail(email);
        if (userOptional.isEmpty()) {
            log.warn("Password reset attempt for non-existent user: {}", email);
            return new PasswordResetResponse("User not found", false);
        }

        User user = userOptional.get();

        // Validate password strength
        if (!isValidPassword(newPassword)) {
            log.warn("Weak password entered for reset by: {}", email);
            return new PasswordResetResponse("Password must be at least 8 characters long", false);
        }

        // Update password
        user.setPassword(passwordEncoder.encode(newPassword));
        userRepository.save(user);

        // Delete token after successful reset
        redisTemplate.delete(redisKey);

        log.info("Password reset successful for user: {}", email);
        return new PasswordResetResponse("Password has been reset successfully", true);
    }

    private boolean isValidPassword(String password) {
        return password.length() >= MIN_PASSWORD_LENGTH;
    }
}
