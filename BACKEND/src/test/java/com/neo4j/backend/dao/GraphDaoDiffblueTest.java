package com.neo4j.backend.dao;

import static org.mockito.Mockito.when;

import com.neo4j.backend.repository.CompanyRepository;
import com.neo4j.backend.repository.GraphRepository;
import com.neo4j.backend.repository.LoanRepository;
import com.neo4j.backend.repository.PersonRepository;
import org.junit.jupiter.api.Disabled;
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

@ContextConfiguration(classes = {GraphDao.class})
@ExtendWith(SpringExtension.class)
@DisabledInAotMode
class GraphDaoDiffblueTest {
    @MockBean
    private CompanyRepository companyRepository;

    @Autowired
    private GraphDao graphDao;

    @MockBean
    private GraphRepository graphRepository;

    @MockBean
    private LoanRepository loanRepository;

    @MockBean
    private PersonRepository personRepository;

    /**
     * Test {@link GraphDao#getGraphData()}.
     * <p>
     * Method under test: {@link GraphDao#getGraphData()}
     */
    @Test
    @DisplayName("Test getGraphData()")
    @Disabled("TODO: Complete this test")
    @Tag("MaintainedByDiffblue")
    void testGetGraphData() {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Reason: No inputs found that don't throw a trivial exception.
        //   Diffblue Cover tried to run the arrange/act section, but the method under
        //   test threw
        //   java.lang.NullPointerException: Cannot invoke "java.lang.Iterable.iterator()" because "iterable" is null
        //       at com.neo4j.backend.dao.GraphDao.processGraphData(GraphDao.java:61)
        //       at com.neo4j.backend.dao.GraphDao.getGraphData(GraphDao.java:39)
        //   See https://diff.blue/R013 to resolve this issue.

        // Arrange
        when(graphRepository.findGraphData()).thenReturn(null);

        // Act
        graphDao.getGraphData();
    }

    /**
     * Test {@link GraphDao#getPersonNetworkByPan(String)}.
     * <p>
     * Method under test: {@link GraphDao#getPersonNetworkByPan(String)}
     */
    @Test
    @DisplayName("Test getPersonNetworkByPan(String)")
    @Disabled("TODO: Complete this test")
    @Tag("MaintainedByDiffblue")
    void testGetPersonNetworkByPan() {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Reason: No inputs found that don't throw a trivial exception.
        //   Diffblue Cover tried to run the arrange/act section, but the method under
        //   test threw
        //   java.lang.NullPointerException: Cannot invoke "java.lang.Iterable.iterator()" because "iterable" is null
        //       at com.neo4j.backend.dao.GraphDao.processGraphData(GraphDao.java:61)
        //       at com.neo4j.backend.dao.GraphDao.getPersonNetworkByPan(GraphDao.java:44)
        //   See https://diff.blue/R013 to resolve this issue.

        // Arrange
        when(personRepository.findPersonNetworkByPan(Mockito.<String>any())).thenReturn(null);

        // Act
        graphDao.getPersonNetworkByPan("42");
    }

    /**
     * Test {@link GraphDao#getCompanyNetworkById(String)}.
     * <p>
     * Method under test: {@link GraphDao#getCompanyNetworkById(String)}
     */
    @Test
    @DisplayName("Test getCompanyNetworkById(String)")
    @Disabled("TODO: Complete this test")
    @Tag("MaintainedByDiffblue")
    void testGetCompanyNetworkById() {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Reason: No inputs found that don't throw a trivial exception.
        //   Diffblue Cover tried to run the arrange/act section, but the method under
        //   test threw
        //   java.lang.NullPointerException: Cannot invoke "java.lang.Iterable.iterator()" because "iterable" is null
        //       at com.neo4j.backend.dao.GraphDao.processGraphData(GraphDao.java:61)
        //       at com.neo4j.backend.dao.GraphDao.getCompanyNetworkById(GraphDao.java:49)
        //   See https://diff.blue/R013 to resolve this issue.

        // Arrange
        when(companyRepository.findCompanyNetworkById(Mockito.<String>any())).thenReturn(null);

        // Act
        graphDao.getCompanyNetworkById("42");
    }

    /**
     * Test {@link GraphDao#getLoanNetworkById(String)}.
     * <p>
     * Method under test: {@link GraphDao#getLoanNetworkById(String)}
     */
    @Test
    @DisplayName("Test getLoanNetworkById(String)")
    @Disabled("TODO: Complete this test")
    @Tag("MaintainedByDiffblue")
    void testGetLoanNetworkById() {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Reason: No inputs found that don't throw a trivial exception.
        //   Diffblue Cover tried to run the arrange/act section, but the method under
        //   test threw
        //   java.lang.NullPointerException: Cannot invoke "java.lang.Iterable.iterator()" because "iterable" is null
        //       at com.neo4j.backend.dao.GraphDao.processGraphData(GraphDao.java:61)
        //       at com.neo4j.backend.dao.GraphDao.getLoanNetworkById(GraphDao.java:54)
        //   See https://diff.blue/R013 to resolve this issue.

        // Arrange
        when(loanRepository.findLoanNetworkById(Mockito.<String>any())).thenReturn(null);

        // Act
        graphDao.getLoanNetworkById("42");
    }
}
