package com.neo4j.backend.controller;

import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.backend.dto.PasswordResetRequest;
import com.neo4j.backend.dto.PasswordResetResponse;
import com.neo4j.backend.dto.PasswordUpdateRequest;
import com.neo4j.backend.service.PasswordResetService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ExtendWith(MockitoExtension.class)
class PasswordResetControllerDiffblueTest {
    @InjectMocks
    private PasswordResetController passwordResetController;

    @Mock
    private PasswordResetService passwordResetService;

    /**
     * Test {@link PasswordResetController#forgotPassword(PasswordResetRequest)}.
     * <ul>
     *   <li>Then status four hundred.</li>
     * </ul>
     * <p>
     * Method under test: {@link PasswordResetController#forgotPassword(PasswordResetRequest)}
     */
    @Test
    @DisplayName("Test forgotPassword(PasswordResetRequest); then status four hundred")
    @Tag("MaintainedByDiffblue")
    void testForgotPassword_thenStatusFourHundred() throws Exception {
        // Arrange
        when(passwordResetService.generateResetToken(Mockito.<PasswordResetRequest>any()))
                .thenReturn(new PasswordResetResponse("Not all who wander are lost", false));

        PasswordResetRequest passwordResetRequest = new PasswordResetRequest();
        passwordResetRequest.setEmail("jane.doe@example.org");
        String content = (new ObjectMapper()).writeValueAsString(passwordResetRequest);
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post("/password/forgot")
                .contentType(MediaType.APPLICATION_JSON)
                .content(content);

        // Act
        ResultActions actualPerformResult = MockMvcBuilders.standaloneSetup(passwordResetController)
                .build()
                .perform(requestBuilder);

        // Assert
        actualPerformResult.andExpect(MockMvcResultMatchers.status().is(400))
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(
                        MockMvcResultMatchers.content().string("{\"message\":\"Not all who wander are lost\",\"success\":false}"));
    }

    /**
     * Test {@link PasswordResetController#resetPassword(PasswordUpdateRequest)}.
     * <ul>
     *   <li>Then status four hundred.</li>
     * </ul>
     * <p>
     * Method under test: {@link PasswordResetController#resetPassword(PasswordUpdateRequest)}
     */
    @Test
    @DisplayName("Test resetPassword(PasswordUpdateRequest); then status four hundred")
    @Tag("MaintainedByDiffblue")
    void testResetPassword_thenStatusFourHundred() throws Exception {
        // Arrange
        when(passwordResetService.resetPassword(Mockito.<PasswordUpdateRequest>any()))
                .thenReturn(new PasswordResetResponse("Not all who wander are lost", false));

        PasswordUpdateRequest passwordUpdateRequest = new PasswordUpdateRequest();
        passwordUpdateRequest.setNewPassword("iloveyou");
        passwordUpdateRequest.setToken("ABC123");
        String content = (new ObjectMapper()).writeValueAsString(passwordUpdateRequest);
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post("/password/reset")
                .contentType(MediaType.APPLICATION_JSON)
                .content(content);

        // Act
        ResultActions actualPerformResult = MockMvcBuilders.standaloneSetup(passwordResetController)
                .build()
                .perform(requestBuilder);

        // Assert
        actualPerformResult.andExpect(MockMvcResultMatchers.status().is(400))
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(
                        MockMvcResultMatchers.content().string("{\"message\":\"Not all who wander are lost\",\"success\":false}"));
    }

    /**
     * Test {@link PasswordResetController#resetPassword(PasswordUpdateRequest)}.
     * <ul>
     *   <li>Then status {@link StatusResultMatchers#isOk()}.</li>
     * </ul>
     * <p>
     * Method under test: {@link PasswordResetController#resetPassword(PasswordUpdateRequest)}
     */
    @Test
    @DisplayName("Test resetPassword(PasswordUpdateRequest); then status isOk()")
    @Tag("MaintainedByDiffblue")
    void testResetPassword_thenStatusIsOk() throws Exception {
        // Arrange
        when(passwordResetService.resetPassword(Mockito.<PasswordUpdateRequest>any()))
                .thenReturn(new PasswordResetResponse("Not all who wander are lost", true));

        PasswordUpdateRequest passwordUpdateRequest = new PasswordUpdateRequest();
        passwordUpdateRequest.setNewPassword("iloveyou");
        passwordUpdateRequest.setToken("ABC123");
        String content = (new ObjectMapper()).writeValueAsString(passwordUpdateRequest);
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post("/password/reset")
                .contentType(MediaType.APPLICATION_JSON)
                .content(content);

        // Act and Assert
        MockMvcBuilders.standaloneSetup(passwordResetController)
                .build()
                .perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(
                        MockMvcResultMatchers.content().string("{\"message\":\"Not all who wander are lost\",\"success\":true}"));
    }

    /**
     * Test {@link PasswordResetController#forgotPassword(PasswordResetRequest)}.
     * <ul>
     *   <li>Then status {@link StatusResultMatchers#isOk()}.</li>
     * </ul>
     * <p>
     * Method under test: {@link PasswordResetController#forgotPassword(PasswordResetRequest)}
     */
    @Test
    @DisplayName("Test forgotPassword(PasswordResetRequest); then status isOk()")
    @Tag("MaintainedByDiffblue")
    void testForgotPassword_thenStatusIsOk() throws Exception {
        // Arrange
        when(passwordResetService.generateResetToken(Mockito.<PasswordResetRequest>any()))
                .thenReturn(new PasswordResetResponse("Not all who wander are lost", true));

        PasswordResetRequest passwordResetRequest = new PasswordResetRequest();
        passwordResetRequest.setEmail("jane.doe@example.org");
        String content = (new ObjectMapper()).writeValueAsString(passwordResetRequest);
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post("/password/forgot")
                .contentType(MediaType.APPLICATION_JSON)
                .content(content);

        // Act and Assert
        MockMvcBuilders.standaloneSetup(passwordResetController)
                .build()
                .perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(
                        MockMvcResultMatchers.content().string("{\"message\":\"Not all who wander are lost\",\"success\":true}"));
    }
}
