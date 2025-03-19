package com.neo4j.backend.controller;

import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.backend.dto.PasswordResetRequest;
import com.neo4j.backend.dto.PasswordResetResponse;
import com.neo4j.backend.dto.PasswordUpdateRequest;
import com.neo4j.backend.exception.GlobalExceptionHandler;
import com.neo4j.backend.service.PasswordResetService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.aot.DisabledInAotMode;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ContextConfiguration(classes = {AuthController.class, GlobalExceptionHandler.class})
@ExtendWith(SpringExtension.class)
@DisabledInAotMode
class AuthControllerDiffblueTest {
    @Autowired
    private AuthController authController;

    @Autowired
    private GlobalExceptionHandler globalExceptionHandler;

    @MockBean
    private PasswordResetService passwordResetService;

    /**
     * Test {@link AuthController#forgotPassword(PasswordResetRequest)}.
     * <p>
     * Method under test: {@link AuthController#forgotPassword(PasswordResetRequest)}
     */
    @Test
    @DisplayName("Test forgotPassword(PasswordResetRequest)")
    @Tag("MaintainedByDiffblue")
    void testForgotPassword() throws Exception {
        // Arrange
        when(passwordResetService.generateResetToken(Mockito.<PasswordResetRequest>any()))
                .thenReturn(new PasswordResetResponse("Not all who wander are lost", true));

        PasswordResetRequest passwordResetRequest = new PasswordResetRequest();
        passwordResetRequest.setEmail("jane.doe@example.org");
        String content = (new ObjectMapper()).writeValueAsString(passwordResetRequest);
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post("/auth-password/forgot")
                .contentType(MediaType.APPLICATION_JSON)
                .content(content);

        // Act and Assert
        MockMvcBuilders.standaloneSetup(authController)
                .setControllerAdvice(globalExceptionHandler)
                .build()
                .perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(
                        MockMvcResultMatchers.content().string("{\"message\":\"Not all who wander are lost\",\"success\":true}"));
    }

    /**
     * Test {@link AuthController#resetPassword(PasswordUpdateRequest)}.
     * <p>
     * Method under test: {@link AuthController#resetPassword(PasswordUpdateRequest)}
     */
    @Test
    @DisplayName("Test resetPassword(PasswordUpdateRequest)")
    @Tag("MaintainedByDiffblue")
    void testResetPassword() throws Exception {
        // Arrange
        when(passwordResetService.resetPassword(Mockito.<PasswordUpdateRequest>any()))
                .thenReturn(new PasswordResetResponse("Not all who wander are lost", true));

        PasswordUpdateRequest passwordUpdateRequest = new PasswordUpdateRequest();
        passwordUpdateRequest.setNewPassword("iloveyou");
        passwordUpdateRequest.setToken("ABC123");
        String content = (new ObjectMapper()).writeValueAsString(passwordUpdateRequest);
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post("/auth-password/reset")
                .contentType(MediaType.APPLICATION_JSON)
                .content(content);

        // Act and Assert
        MockMvcBuilders.standaloneSetup(authController)
                .setControllerAdvice(globalExceptionHandler)
                .build()
                .perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(
                        MockMvcResultMatchers.content().string("{\"message\":\"Not all who wander are lost\",\"success\":true}"));
    }
}
