package com.neo4j.backend.service;


import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class EmailService {

    private final JavaMailSender mailSender;

    @Value("${spring.mail.username}")
    private String fromEmail;

    @Value("${app.frontend.url:http://localhost:5173}")
    private String frontendUrl;

    public void sendPasswordResetEmail(String toEmail, String token) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true);

            helper.setFrom(fromEmail);
            helper.setTo(toEmail);
            helper.setSubject("Password Reset Request");

            String resetLink = frontendUrl + "/reset-password?token=" + token;

            String emailContent =
                    "<div style='font-family: Arial, sans-serif; padding: 20px;'>" +
                            "<h2>Password Reset Request</h2>" +
                            "<p>You recently requested to reset your password. Click the link below to reset it:</p>" +
                            "<p><a href='" + resetLink + "' style='background-color: #4CAF50; color: white; padding: 10px 20px; " +
                            "text-decoration: none; border-radius: 5px;'>Reset Password</a></p>" +
                            "<p>This link will expire in 30 minutes.</p>" +
                            "<p>If you did not request a password reset, please ignore this email.</p>" +
                            "</div>";

            helper.setText(emailContent, true);

            log.info("Attempting to send email to: {} with reset link: {}", toEmail, resetLink);
            mailSender.send(message);
            log.info("Password reset email sent successfully to: {}", toEmail);
        } catch (MessagingException e) {
            log.error("MessagingException when sending email: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send password reset email: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Unexpected error when sending email: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send password reset email: " + e.getMessage(), e);
        }
    }
}