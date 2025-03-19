package com.neo4j.backend.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.driver.Driver;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.aot.DisabledInAotMode;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ContextConfiguration(classes = {Neo4jConfig.class})
@ExtendWith(SpringExtension.class)
@DisabledInAotMode
class Neo4jConfigDiffblueTest {
    @MockBean
    private Driver driver;

    @Autowired
    private Neo4jConfig neo4jConfig;

    @MockBean
    private ObjectProvider objectProvider;

    /**
     * Test {@link Neo4jConfig#driver()}.
     * <p>
     * Method under test: {@link Neo4jConfig#driver()}
     */
    @Test
    @DisplayName("Test driver()")
    @Tag("MaintainedByDiffblue")
    void testDriver() {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Diffblue AI was unable to find a test

        // Arrange and Act
        neo4jConfig.driver();
    }
}
