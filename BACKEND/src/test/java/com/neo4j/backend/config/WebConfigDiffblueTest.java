package com.neo4j.backend.config;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.servlet.config.annotation.CorsRegistry;

@ContextConfiguration(classes = {WebConfig.class})
@ExtendWith(SpringExtension.class)
class WebConfigDiffblueTest {
    @Autowired
    private WebConfig webConfig;

    /**
     * Test {@link WebConfig#addCorsMappings(CorsRegistry)}.
     * <ul>
     *   <li>When {@link CorsRegistry} (default constructor).</li>
     * </ul>
     * <p>
     * Method under test: {@link WebConfig#addCorsMappings(CorsRegistry)}
     */
    @Test
    @DisplayName("Test addCorsMappings(CorsRegistry); when CorsRegistry (default constructor)")
    @Tag("MaintainedByDiffblue")
    void testAddCorsMappings_whenCorsRegistry() {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Diffblue AI was unable to find a test

        // Arrange and Act
        webConfig.addCorsMappings(new CorsRegistry());
    }

    /**
     * Test {@link WebConfig#addCorsMappings(CorsRegistry)}.
     * <ul>
     *   <li>When {@code null}.</li>
     * </ul>
     * <p>
     * Method under test: {@link WebConfig#addCorsMappings(CorsRegistry)}
     */
    @Test
    @DisplayName("Test addCorsMappings(CorsRegistry); when 'null'")
    @Disabled("TODO: Complete this test")
    @Tag("MaintainedByDiffblue")
    void testAddCorsMappings_whenNull() {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Reason: No inputs found that don't throw a trivial exception.
        //   Diffblue Cover tried to run the arrange/act section, but the method under
        //   test threw
        //   java.lang.NullPointerException: Cannot invoke "org.springframework.web.servlet.config.annotation.CorsRegistry.addMapping(String)" because "registry" is null
        //       at com.neo4j.backend.config.WebConfig.addCorsMappings(WebConfig.java:12)
        //   See https://diff.blue/R013 to resolve this issue.

        // Arrange and Act
        webConfig.addCorsMappings(null);
    }
}
