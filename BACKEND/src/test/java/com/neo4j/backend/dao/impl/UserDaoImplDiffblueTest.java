package com.neo4j.backend.dao.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.neo4j.backend.dto.UserRequest;
import com.neo4j.backend.dto.UserResponse;
import com.neo4j.backend.model.User;
import com.neo4j.backend.repository.UserRepository;

import java.util.Optional;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ContextConfiguration(classes = {UserDaoImpl.class})
@ExtendWith(SpringExtension.class)
@ExtendWith(MockitoExtension.class)
class UserDaoImplDiffblueTest {
    @Autowired
    private UserDaoImpl userDaoImpl;

    @Mock
    private PasswordEncoder passwordEncoder;

    @Mock
    private UserRepository userRepository;

    /**
     * Test {@link UserDaoImpl#signup(UserRequest)}.
     * <p>
     * Method under test: {@link UserDaoImpl#signup(UserRequest)}
     */
    @Test
    @DisplayName("Test signup(UserRequest)")
    @Disabled("TODO: Complete this test")
    @Tag("MaintainedByDiffblue")
    void testSignup() {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Reason: No inputs found that don't throw a trivial exception.
        //   Diffblue Cover tried to run the arrange/act section, but the method under
        //   test threw
        //   java.lang.NullPointerException: password is marked non-null but is null
        //       at com.neo4j.backend.model.User.setPassword(User.java:11)
        //       at com.neo4j.backend.dao.impl.UserDaoImpl.signup(UserDaoImpl.java:45)
        //   See https://diff.blue/R013 to resolve this issue.

        // Arrange
        // TODO: Populate arranged inputs
        UserRequest request = null;

        // Act
        UserResponse actualSignupResult = this.userDaoImpl.signup(request);

        // Assert
        // TODO: Add assertions on result
    }

    /**
     * Test {@link UserDaoImpl#login(UserRequest)}.
     * <p>
     * Method under test: {@link UserDaoImpl#login(UserRequest)}
     */
    @Test
    @DisplayName("Test login(UserRequest)")
    @Tag("MaintainedByDiffblue")
    void testLogin() {
        // Arrange
        User buildResult = User.builder().email("jane.doe@example.org").id("42").password("iloveyou").tasks(null).build();
        buildResult.setEmail("jane.doe@example.org");
        buildResult.setId("42");
        buildResult.setPassword("iloveyou");
        Optional<User> ofResult = Optional.of(buildResult);
        when(userRepository.findByEmail(Mockito.<String>any())).thenReturn(ofResult);
        when(passwordEncoder.matches(Mockito.<CharSequence>any(), Mockito.<String>any())).thenReturn(true);

        UserRequest request = new UserRequest();
        request.setEmail("jane.doe@example.org");
        request.setPassword("iloveyou");

        // Act
        UserResponse actualLoginResult = userDaoImpl.login(request);

        // Assert
        verify(userRepository).findByEmail(eq("jane.doe@example.org"));
        verify(passwordEncoder).matches(isA(CharSequence.class), eq("iloveyou"));
        assertEquals("Login Successful", actualLoginResult.getMessage());
        assertNull(actualLoginResult.getEmail());
        assertTrue(actualLoginResult.getTasks().isEmpty());
    }

    /**
     * Test {@link UserDaoImpl#login(UserRequest)}.
     * <ul>
     *   <li>Given {@link User#User()} Email is {@code jane.doe@example.org}.</li>
     * </ul>
     * <p>
     * Method under test: {@link UserDaoImpl#login(UserRequest)}
     */
    @Test
    @DisplayName("Test login(UserRequest); given User() Email is 'jane.doe@example.org'")
    @Tag("MaintainedByDiffblue")
    void testLogin_givenUserEmailIsJaneDoeExampleOrg() {
        // Arrange
        User user = new User();
        user.setEmail("jane.doe@example.org");
        user.setId("42");
        user.setPassword("iloveyou");
        Optional<User> ofResult = Optional.of(user);
        when(userRepository.findByEmail(Mockito.<String>any())).thenReturn(ofResult);
        when(passwordEncoder.matches(Mockito.<CharSequence>any(), Mockito.<String>any())).thenReturn(true);

        UserRequest request = new UserRequest();
        request.setEmail("jane.doe@example.org");
        request.setPassword("iloveyou");

        // Act
        UserResponse actualLoginResult = userDaoImpl.login(request);

        // Assert
        verify(userRepository).findByEmail(eq("jane.doe@example.org"));
        verify(passwordEncoder).matches(isA(CharSequence.class), eq("iloveyou"));
        assertEquals("Login Successful", actualLoginResult.getMessage());
        assertNull(actualLoginResult.getEmail());
        assertTrue(actualLoginResult.getTasks().isEmpty());
    }

    /**
     * Test {@link UserDaoImpl#addTask(String, String)}.
     * <p>
     * Method under test: {@link UserDaoImpl#addTask(String, String)}
     */
    @Test
    @DisplayName("Test addTask(String, String)")
    @Disabled("TODO: Complete this test")
    @Tag("MaintainedByDiffblue")
    void testAddTask() {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Diffblue AI was unable to find a test

        // Arrange
        // TODO: Populate arranged inputs
        String email = "";
        String task = "";

        // Act
        UserResponse actualAddTaskResult = this.userDaoImpl.addTask(email, task);

        // Assert
        // TODO: Add assertions on result
    }
}
