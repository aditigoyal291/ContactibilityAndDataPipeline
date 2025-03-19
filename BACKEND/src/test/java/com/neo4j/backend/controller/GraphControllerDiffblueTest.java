package com.neo4j.backend.controller;

import static org.mockito.Mockito.when;

import com.neo4j.backend.dto.GraphDataDto;
import com.neo4j.backend.exception.GlobalExceptionHandler;
import com.neo4j.backend.service.GraphService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.aot.DisabledInAotMode;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ContextConfiguration(classes = {GraphController.class, GlobalExceptionHandler.class})
@ExtendWith(SpringExtension.class)
@DisabledInAotMode
class GraphControllerDiffblueTest {
    @Autowired
    private GlobalExceptionHandler globalExceptionHandler;

    @Autowired
    private GraphController graphController;

    @MockBean
    private GraphService graphService;

    /**
     * Test {@link GraphController#getFullGraph()}.
     * <p>
     * Method under test: {@link GraphController#getFullGraph()}
     */
    @Test
    @DisplayName("Test getFullGraph()")
    @Tag("MaintainedByDiffblue")
    void testGetFullGraph() throws Exception {
        // Arrange
        when(graphService.getFullGraph()).thenReturn(new GraphDataDto());
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.get("/api/graph");

        // Act and Assert
        MockMvcBuilders.standaloneSetup(graphController)
                .setControllerAdvice(globalExceptionHandler)
                .build()
                .perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content().string("{\"nodes\":null,\"links\":null}"));
    }

    /**
     * Test {@link GraphController#getPersonByPan(String)}.
     * <p>
     * Method under test: {@link GraphController#getPersonByPan(String)}
     */
    @Test
    @DisplayName("Test getPersonByPan(String)")
    @Tag("MaintainedByDiffblue")
    void testGetPersonByPan() throws Exception {
        // Arrange
        when(graphService.getPersonByPan(Mockito.<String>any())).thenReturn(new GraphDataDto());
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.get("/api/node/pan/{panNumber}", "42");

        // Act and Assert
        MockMvcBuilders.standaloneSetup(graphController)
                .setControllerAdvice(globalExceptionHandler)
                .build()
                .perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content().string("{\"nodes\":null,\"links\":null}"));
    }

    /**
     * Test {@link GraphController#getCompanyById(String)}.
     * <p>
     * Method under test: {@link GraphController#getCompanyById(String)}
     */
    @Test
    @DisplayName("Test getCompanyById(String)")
    @Tag("MaintainedByDiffblue")
    void testGetCompanyById() throws Exception {
        // Arrange
        when(graphService.getCompanyById(Mockito.<String>any())).thenReturn(new GraphDataDto());
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.get("/api/node/company/{companyId}", "42");

        // Act and Assert
        MockMvcBuilders.standaloneSetup(graphController)
                .setControllerAdvice(globalExceptionHandler)
                .build()
                .perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content().string("{\"nodes\":null,\"links\":null}"));
    }

    /**
     * Test {@link GraphController#getLoanById(String)}.
     * <p>
     * Method under test: {@link GraphController#getLoanById(String)}
     */
    @Test
    @DisplayName("Test getLoanById(String)")
    @Tag("MaintainedByDiffblue")
    void testGetLoanById() throws Exception {
        // Arrange
        when(graphService.getLoanById(Mockito.<String>any())).thenReturn(new GraphDataDto());
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.get("/api/node/loan/{loanId}", "42");

        // Act and Assert
        MockMvcBuilders.standaloneSetup(graphController)
                .setControllerAdvice(globalExceptionHandler)
                .build()
                .perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content().string("{\"nodes\":null,\"links\":null}"));
    }
}
