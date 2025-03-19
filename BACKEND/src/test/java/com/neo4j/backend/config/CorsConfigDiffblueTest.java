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
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.filter.CorsFilter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;

@ContextConfiguration(classes = {CorsConfig.class})
@ExtendWith(SpringExtension.class)
class CorsConfigDiffblueTest {
    @Autowired
    private CorsConfig corsConfig;

    /**
     * Test {@link CorsConfig#addCorsMappings(CorsRegistry)}.
     * <ul>
     *   <li>When {@link CorsRegistry} (default constructor).</li>
     * </ul>
     * <p>
     * Method under test: {@link CorsConfig#addCorsMappings(CorsRegistry)}
     */
    @Test
    @DisplayName("Test addCorsMappings(CorsRegistry); when CorsRegistry (default constructor)")
    @Tag("MaintainedByDiffblue")
    void testAddCorsMappings_whenCorsRegistry() {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Diffblue AI was unable to find a test

        // Arrange and Act
        corsConfig.addCorsMappings(new CorsRegistry());
    }

    /**
     * Test {@link CorsConfig#addCorsMappings(CorsRegistry)}.
     * <ul>
     *   <li>When {@code null}.</li>
     * </ul>
     * <p>
     * Method under test: {@link CorsConfig#addCorsMappings(CorsRegistry)}
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
        //       at com.neo4j.backend.config.CorsConfig.addCorsMappings(CorsConfig.java:16)
        //   See https://diff.blue/R013 to resolve this issue.

        // Arrange and Act
        corsConfig.addCorsMappings(null);
    }

    /**
     * Test {@link CorsConfig#corsFilter()}.
     * <p>
     * Method under test: {@link CorsConfig#corsFilter()}
     */
    @Test
    @DisplayName("Test corsFilter()")
    @Tag("MaintainedByDiffblue")
    void testCorsFilter() {
        // Arrange and Act
        CorsFilter actualCorsFilterResult = corsConfig.corsFilter();

        // Assert
        Environment environment = actualCorsFilterResult.getEnvironment();
        assertTrue(((StandardEnvironment) environment).getConversionService() instanceof DefaultConversionService);
        assertTrue(environment instanceof StandardEnvironment);
        assertNull(actualCorsFilterResult.getFilterConfig());
        assertEquals(0, environment.getActiveProfiles().length);
        assertEquals(1, environment.getDefaultProfiles().length);
        Map<String, Object> systemEnvironment = ((StandardEnvironment) environment).getSystemEnvironment();
        assertEquals(53, systemEnvironment.size());
        Map<String, Object> systemProperties = ((StandardEnvironment) environment).getSystemProperties();
        assertEquals(83, systemProperties.size());
        assertTrue(systemEnvironment.containsKey("PROCESSOR_LEVEL"));
        assertTrue(systemEnvironment.containsKey("RegionCode"));
        assertTrue(systemEnvironment.containsKey("SESSIONNAME"));
        assertTrue(systemEnvironment.containsKey("USERDOMAIN_ROAMINGPROFILE"));
        assertTrue(systemProperties.containsKey("cover.jar.path"));
        assertTrue(systemProperties.containsKey("java.specification.version"));
        assertTrue(systemProperties.containsKey("kotlinx.coroutines.debug"));
        assertTrue(systemProperties.containsKey("sun.cpu.isalist"));
    }
}
