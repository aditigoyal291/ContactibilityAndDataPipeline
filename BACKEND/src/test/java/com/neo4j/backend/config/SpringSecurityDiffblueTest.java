package com.neo4j.backend.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.config.ObjectPostProcessor;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

@ContextConfiguration(classes = {SpringSecurity.class})
@ExtendWith(SpringExtension.class)
class SpringSecurityDiffblueTest {
    @Autowired
    private SpringSecurity springSecurity;

    /**
     * Test {@link SpringSecurity#securityFilterChain(HttpSecurity)}.
     * <p>
     * Method under test: {@link SpringSecurity#securityFilterChain(HttpSecurity)}
     */
    @Test
    @DisplayName("Test securityFilterChain(HttpSecurity)")
    @Disabled("TODO: Complete this test")
    @Tag("MaintainedByDiffblue")
    void testSecurityFilterChain() throws Exception {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Reason: No inputs found that don't throw a trivial exception.
        //   Diffblue Cover tried to run the arrange/act section, but the method under
        //   test threw
        //   java.lang.IllegalArgumentException: objectPostProcessor cannot be null
        //   See https://diff.blue/R013 to resolve this issue.

        // Arrange and Act
        springSecurity.securityFilterChain(new HttpSecurity((ObjectPostProcessor<Object>) null,
                new AuthenticationManagerBuilder((ObjectPostProcessor<Object>) null), null));
    }

    /**
     * Test {@link SpringSecurity#corsConfigurationSource()}.
     * <p>
     * Method under test: {@link SpringSecurity#corsConfigurationSource()}
     */
    @Test
    @DisplayName("Test corsConfigurationSource()")
    @Tag("MaintainedByDiffblue")
    void testCorsConfigurationSource() {
        // Arrange and Act
        CorsConfigurationSource actualCorsConfigurationSourceResult = springSecurity.corsConfigurationSource();

        // Assert
        assertTrue(actualCorsConfigurationSourceResult instanceof UrlBasedCorsConfigurationSource);
        Map<String, CorsConfiguration> corsConfigurations = ((UrlBasedCorsConfigurationSource) actualCorsConfigurationSourceResult)
                .getCorsConfigurations();
        assertEquals(1, corsConfigurations.size());
        CorsConfiguration getResult = corsConfigurations.get("/**");
        assertNull(getResult.getAllowPrivateNetwork());
        assertNull(getResult.getMaxAge());
        assertNull(getResult.getAllowedOriginPatterns());
        assertNull(getResult.getExposedHeaders());
        assertEquals(1, getResult.getAllowedHeaders().size());
        assertEquals(1, getResult.getAllowedOrigins().size());
        assertEquals(5, getResult.getAllowedMethods().size());
        assertTrue(getResult.getAllowCredentials());
    }

    /**
     * Test {@link SpringSecurity#passwordEncoder()}.
     * <p>
     * Method under test: {@link SpringSecurity#passwordEncoder()}
     */
    @Test
    @DisplayName("Test passwordEncoder()")
    @Tag("MaintainedByDiffblue")
    void testPasswordEncoder() {
        // Arrange, Act and Assert
        assertTrue(springSecurity.passwordEncoder() instanceof BCryptPasswordEncoder);
    }
}
