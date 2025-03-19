package com.neo4j.backend.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.backend.dao.impl.UserDaoImpl;
import com.neo4j.backend.dto.UserRequest;
import com.neo4j.backend.dto.UserResponse;
import com.neo4j.backend.exception.GlobalExceptionHandler;
import com.neo4j.backend.model.User;
import com.neo4j.backend.repository.UserRepository;
import com.neo4j.backend.service.impl.UserServiceImpl;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.LdapShaPasswordEncoder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.aot.DisabledInAotMode;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ContextConfiguration(classes = {UserController.class, GlobalExceptionHandler.class})
@ExtendWith(SpringExtension.class)
@DisabledInAotMode
@ExtendWith(MockitoExtension.class)
class UserControllerDiffblueTest {
    @Autowired
    private GlobalExceptionHandler globalExceptionHandler;

    @Autowired
    private UserController userController;

    @MockBean
    private UserServiceImpl userServiceImpl;

    /**
     * Test {@link UserController#signup(UserRequest)}.
     * <ul>
     *   <li>Then status four hundred nine.</li>
     * </ul>
     * <p>
     * Method under test: {@link UserController#signup(UserRequest)}
     */
    @Test
    @DisplayName("Test signup(UserRequest); then status four hundred nine")
    @Tag("MaintainedByDiffblue")
    void testSignup_thenStatusFourHundredNine() throws Exception {
        // Arrange
        when(userServiceImpl.signup(Mockito.<UserRequest>any()))
                .thenReturn(new UserResponse("User Already Exists", "ABC123"));

        UserRequest userRequest = new UserRequest();
        userRequest.setEmail("jane.doe@example.org");
        userRequest.setPassword("iloveyou");
        String content = (new ObjectMapper()).writeValueAsString(userRequest);
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post("/user/signup")
                .contentType(MediaType.APPLICATION_JSON)
                .content(content);

        // Act
        ResultActions actualPerformResult = MockMvcBuilders.standaloneSetup(userController).build().perform(requestBuilder);

        // Assert
        actualPerformResult.andExpect(MockMvcResultMatchers.status().is(409))
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content()
                        .string("{\"message\":\"User Already Exists\",\"token\":\"ABC123\",\"email\":null,\"tasks\":null}"));
    }

    /**
     * Test {@link UserController#signup(UserRequest)}.
     * <ul>
     *   <li>Then status {@link StatusResultMatchers#isOk()}.</li>
     * </ul>
     * <p>
     * Method under test: {@link UserController#signup(UserRequest)}
     */
    @Test
    @DisplayName("Test signup(UserRequest); then status isOk()")
    @Tag("MaintainedByDiffblue")
    void testSignup_thenStatusIsOk() throws Exception {
        // Arrange
        when(userServiceImpl.signup(Mockito.<UserRequest>any()))
                .thenReturn(new UserResponse("Not all who wander are lost", "ABC123"));

        UserRequest userRequest = new UserRequest();
        userRequest.setEmail("jane.doe@example.org");
        userRequest.setPassword("iloveyou");
        String content = (new ObjectMapper()).writeValueAsString(userRequest);
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post("/user/signup")
                .contentType(MediaType.APPLICATION_JSON)
                .content(content);

        // Act and Assert
        MockMvcBuilders.standaloneSetup(userController)
                .build()
                .perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content()
                        .string(
                                "{\"message\":\"Not all who wander are lost\",\"token\":\"ABC123\",\"email\":null,\"tasks\":null}"));
    }

    /**
     * Test {@link UserController#login(UserRequest)}.
     * <p>
     * Method under test: {@link UserController#login(UserRequest)}
     */
    @Test
    @DisplayName("Test login(UserRequest)")
    @Tag("MaintainedByDiffblue")
    void testLogin() {
        //   Diffblue Cover was unable to create a Spring-specific test for this Spring method.
        //   Run dcover create --keep-partial-tests to gain insights into why
        //   a non-Spring test was created.

        // Arrange
        UserServiceImpl userService = mock(UserServiceImpl.class);
        UserResponse userResponse = new UserResponse("Not all who wander are lost", "ABC123");

        when(userService.login(Mockito.<UserRequest>any())).thenReturn(userResponse);
        UserController userController = new UserController(userService);

        UserRequest request = new UserRequest();
        request.setEmail("jane.doe@example.org");
        request.setPassword("iloveyou");

        // Act
        ResponseEntity<UserResponse> actualLoginResult = userController.login(request);

        // Assert
        verify(userService).login(isA(UserRequest.class));
        HttpStatusCode statusCode = actualLoginResult.getStatusCode();
        assertTrue(statusCode instanceof HttpStatus);
        assertEquals(200, actualLoginResult.getStatusCodeValue());
        assertEquals(HttpStatus.OK, statusCode);
        assertSame(userResponse, actualLoginResult.getBody());
    }

    /**
     * Test {@link UserController#login(UserRequest)}.
     * <ul>
     *   <li>Then return Body Message is {@code Invalid Credentials}.</li>
     * </ul>
     * <p>
     * Method under test: {@link UserController#login(UserRequest)}
     */
    @Test
    @DisplayName("Test login(UserRequest); then return Body Message is 'Invalid Credentials'")
    @Tag("MaintainedByDiffblue")
    void testLogin_thenReturnBodyMessageIsInvalidCredentials() {
        //   Diffblue Cover was unable to create a Spring-specific test for this Spring method.
        //   Run dcover create --keep-partial-tests to gain insights into why
        //   a non-Spring test was created.

        // Arrange
        User user = new User();
        user.setEmail("jane.doe@example.org");
        user.setId("42");
        user.setPassword("iloveyou");
        Optional<User> ofResult = Optional.of(user);
        UserRepository userRepository = mock(UserRepository.class);
        when(userRepository.findByEmail(Mockito.<String>any())).thenReturn(ofResult);
        UserController userController = new UserController(
                new UserServiceImpl(new UserDaoImpl(userRepository, new BCryptPasswordEncoder())));

        UserRequest request = new UserRequest();
        request.setEmail("jane.doe@example.org");
        request.setPassword("iloveyou");

        // Act
        ResponseEntity<UserResponse> actualLoginResult = userController.login(request);

        // Assert
        verify(userRepository).findByEmail(eq("jane.doe@example.org"));
        HttpStatusCode statusCode = actualLoginResult.getStatusCode();
        assertTrue(statusCode instanceof HttpStatus);
        UserResponse body = actualLoginResult.getBody();
        assertEquals("Invalid Credentials", body.getMessage());
        assertNull(body.getToken());
        assertEquals(401, actualLoginResult.getStatusCodeValue());
        assertEquals(HttpStatus.UNAUTHORIZED, statusCode);
    }

    /**
     * Test {@link UserController#login(UserRequest)}.
     * <ul>
     *   <li>Then return Body Message is {@code Login Successful}.</li>
     * </ul>
     * <p>
     * Method under test: {@link UserController#login(UserRequest)}
     */
    @Test
    @DisplayName("Test login(UserRequest); then return Body Message is 'Login Successful'")
    @Tag("MaintainedByDiffblue")
    void testLogin_thenReturnBodyMessageIsLoginSuccessful() {
        //   Diffblue Cover was unable to create a Spring-specific test for this Spring method.
        //   Run dcover create --keep-partial-tests to gain insights into why
        //   a non-Spring test was created.

        // Arrange
        User user = new User();
        user.setEmail("jane.doe@example.org");
        user.setId("42");
        user.setPassword("iloveyou");
        Optional<User> ofResult = Optional.of(user);
        UserRepository userRepository = mock(UserRepository.class);
        when(userRepository.findByEmail(Mockito.<String>any())).thenReturn(ofResult);
        UserController userController = new UserController(
                new UserServiceImpl(new UserDaoImpl(userRepository, new LdapShaPasswordEncoder())));

        UserRequest request = new UserRequest();
        request.setEmail("jane.doe@example.org");
        request.setPassword("iloveyou");

        // Act
        ResponseEntity<UserResponse> actualLoginResult = userController.login(request);

        // Assert
        verify(userRepository).findByEmail(eq("jane.doe@example.org"));
        HttpStatusCode statusCode = actualLoginResult.getStatusCode();
        assertTrue(statusCode instanceof HttpStatus);
        UserResponse body = actualLoginResult.getBody();
        assertEquals("Login Successful", body.getMessage());
        assertEquals(200, actualLoginResult.getStatusCodeValue());
        assertEquals(HttpStatus.OK, statusCode);
        assertTrue(body.getTasks().isEmpty());
    }

    /**
     * Test {@link UserController#login(UserRequest)}.
     * <ul>
     *   <li>Then return Body Message is {@code User Does not exist. Please Signup}.</li>
     * </ul>
     * <p>
     * Method under test: {@link UserController#login(UserRequest)}
     */
    @Test
    @DisplayName("Test login(UserRequest); then return Body Message is 'User Does not exist. Please Signup'")
    @Tag("MaintainedByDiffblue")
    void testLogin_thenReturnBodyMessageIsUserDoesNotExistPleaseSignup() {
        //   Diffblue Cover was unable to create a Spring-specific test for this Spring method.
        //   Run dcover create --keep-partial-tests to gain insights into why
        //   a non-Spring test was created.

        // Arrange
        UserRepository userRepository = mock(UserRepository.class);
        Optional<User> emptyResult = Optional.empty();
        when(userRepository.findByEmail(Mockito.<String>any())).thenReturn(emptyResult);
        UserController userController = new UserController(
                new UserServiceImpl(new UserDaoImpl(userRepository, new BCryptPasswordEncoder())));

        UserRequest request = new UserRequest();
        request.setEmail("jane.doe@example.org");
        request.setPassword("iloveyou");

        // Act
        ResponseEntity<UserResponse> actualLoginResult = userController.login(request);

        // Assert
        verify(userRepository).findByEmail(eq("jane.doe@example.org"));
        HttpStatusCode statusCode = actualLoginResult.getStatusCode();
        assertTrue(statusCode instanceof HttpStatus);
        UserResponse body = actualLoginResult.getBody();
        assertEquals("User Does not exist. Please Signup", body.getMessage());
        assertNull(body.getToken());
        assertEquals(404, actualLoginResult.getStatusCodeValue());
        assertEquals(HttpStatus.NOT_FOUND, statusCode);
    }

    /**
     * Test {@link UserController#addTask(String, String)}.
     * <p>
     * Method under test: {@link UserController#addTask(String, String)}
     */
    @Test
    @DisplayName("Test addTask(String, String)")
    @Tag("MaintainedByDiffblue")
    void testAddTask() throws Exception {
        // Arrange
        when(userServiceImpl.addTask(Mockito.<String>any(), Mockito.<String>any()))
                .thenReturn(new UserResponse("Not all who wander are lost", "ABC123"));
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.put("/user/addtask")
                .param("email", "foo")
                .param("task", "foo");

        // Act and Assert
        MockMvcBuilders.standaloneSetup(userController)
                .setControllerAdvice(globalExceptionHandler)
                .build()
                .perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content()
                        .string(
                                "{\"message\":\"Not all who wander are lost\",\"token\":\"ABC123\",\"email\":null,\"tasks\":null}"));
    }
}
