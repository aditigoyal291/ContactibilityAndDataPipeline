package com.neo4j.backend.config;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

import org.apache.catalina.connector.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletRequest;

class JwtAuthenticationFilterDiffblueTest {
    /**
     * Test {@link JwtAuthenticationFilter#doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain)}.
     * <ul>
     *   <li>Given {@code /user/login}.</li>
     * </ul>
     * <p>
     * Method under test: {@link JwtAuthenticationFilter#doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain)}
     */
    @Test
    @DisplayName("Test doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain); given '/user/login'")
    @Tag("MaintainedByDiffblue")
    void testDoFilterInternal_givenUserLogin() throws ServletException, IOException {
        // Arrange
        JwtAuthenticationFilter jwtAuthenticationFilter = new JwtAuthenticationFilter();

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setServletPath("/user/login");
        Response response = new Response();
        FilterChain chain = mock(FilterChain.class);
        doNothing().when(chain).doFilter(Mockito.<ServletRequest>any(), Mockito.<ServletResponse>any());

        // Act
        jwtAuthenticationFilter.doFilterInternal(request, response, chain);

        // Assert
        verify(chain).doFilter(isA(ServletRequest.class), isA(ServletResponse.class));
    }

    /**
     * Test {@link JwtAuthenticationFilter#doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain)}.
     * <ul>
     *   <li>Given {@code /user/login}.</li>
     *   <li>Then throw {@link ServletException}.</li>
     * </ul>
     * <p>
     * Method under test: {@link JwtAuthenticationFilter#doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain)}
     */
    @Test
    @DisplayName("Test doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain); given '/user/login'; then throw ServletException")
    @Tag("MaintainedByDiffblue")
    void testDoFilterInternal_givenUserLogin_thenThrowServletException() throws ServletException, IOException {
        // Arrange
        JwtAuthenticationFilter jwtAuthenticationFilter = new JwtAuthenticationFilter();

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setServletPath("/user/login");
        Response response = new Response();
        FilterChain chain = mock(FilterChain.class);
        doThrow(new ServletException("An error occurred")).when(chain)
                .doFilter(Mockito.<ServletRequest>any(), Mockito.<ServletResponse>any());

        // Act and Assert
        assertThrows(ServletException.class, () -> jwtAuthenticationFilter.doFilterInternal(request, response, chain));
        verify(chain).doFilter(isA(ServletRequest.class), isA(ServletResponse.class));
    }

    /**
     * Test {@link JwtAuthenticationFilter#doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain)}.
     * <ul>
     *   <li>Given {@code /user/signup}.</li>
     * </ul>
     * <p>
     * Method under test: {@link JwtAuthenticationFilter#doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain)}
     */
    @Test
    @DisplayName("Test doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain); given '/user/signup'")
    @Tag("MaintainedByDiffblue")
    void testDoFilterInternal_givenUserSignup() throws ServletException, IOException {
        // Arrange
        JwtAuthenticationFilter jwtAuthenticationFilter = new JwtAuthenticationFilter();

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setServletPath("/user/signup");
        Response response = new Response();
        FilterChain chain = mock(FilterChain.class);
        doNothing().when(chain).doFilter(Mockito.<ServletRequest>any(), Mockito.<ServletResponse>any());

        // Act
        jwtAuthenticationFilter.doFilterInternal(request, response, chain);

        // Assert
        verify(chain).doFilter(isA(ServletRequest.class), isA(ServletResponse.class));
    }

    /**
     * Test {@link JwtAuthenticationFilter#doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain)}.
     * <ul>
     *   <li>Then throw {@link ServletException}.</li>
     * </ul>
     * <p>
     * Method under test: {@link JwtAuthenticationFilter#doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain)}
     */
    @Test
    @DisplayName("Test doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain); then throw ServletException")
    @Tag("MaintainedByDiffblue")
    void testDoFilterInternal_thenThrowServletException() throws ServletException, IOException {
        // Arrange
        JwtAuthenticationFilter jwtAuthenticationFilter = new JwtAuthenticationFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        Response response = new Response();
        FilterChain chain = mock(FilterChain.class);
        doThrow(new ServletException("An error occurred")).when(chain)
                .doFilter(Mockito.<ServletRequest>any(), Mockito.<ServletResponse>any());

        // Act and Assert
        assertThrows(ServletException.class, () -> jwtAuthenticationFilter.doFilterInternal(request, response, chain));
        verify(chain).doFilter(isA(ServletRequest.class), isA(ServletResponse.class));
    }

    /**
     * Test {@link JwtAuthenticationFilter#doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain)}.
     * <ul>
     *   <li>When {@link MockHttpServletRequest#MockHttpServletRequest()}.</li>
     *   <li>Then calls {@link FilterChain#doFilter(ServletRequest, ServletResponse)}.</li>
     * </ul>
     * <p>
     * Method under test: {@link JwtAuthenticationFilter#doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain)}
     */
    @Test
    @DisplayName("Test doFilterInternal(HttpServletRequest, HttpServletResponse, FilterChain); when MockHttpServletRequest(); then calls doFilter(ServletRequest, ServletResponse)")
    @Tag("MaintainedByDiffblue")
    void testDoFilterInternal_whenMockHttpServletRequest_thenCallsDoFilter() throws ServletException, IOException {
        // Arrange
        JwtAuthenticationFilter jwtAuthenticationFilter = new JwtAuthenticationFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        Response response = new Response();
        FilterChain chain = mock(FilterChain.class);
        doNothing().when(chain).doFilter(Mockito.<ServletRequest>any(), Mockito.<ServletResponse>any());

        // Act
        jwtAuthenticationFilter.doFilterInternal(request, response, chain);

        // Assert
        verify(chain).doFilter(isA(ServletRequest.class), isA(ServletResponse.class));
    }
}
