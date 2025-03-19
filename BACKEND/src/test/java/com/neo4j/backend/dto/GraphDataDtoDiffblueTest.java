package com.neo4j.backend.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class GraphDataDtoDiffblueTest {
    /**
     * Test {@link GraphDataDto#equals(Object)}, and {@link GraphDataDto#hashCode()}.
     * <ul>
     *   <li>When other is equal.</li>
     *   <li>Then return equal.</li>
     * </ul>
     * <p>
     * Methods under test:
     * <ul>
     *   <li>{@link GraphDataDto#equals(Object)}
     *   <li>{@link GraphDataDto#hashCode()}
     * </ul>
     */
    @Test
    @DisplayName("Test equals(Object), and hashCode(); when other is equal; then return equal")
    @Tag("MaintainedByDiffblue")
    void testEqualsAndHashCode_whenOtherIsEqual_thenReturnEqual() {
        // Arrange
        GraphDataDto graphDataDto = new GraphDataDto();
        GraphDataDto graphDataDto2 = new GraphDataDto();

        // Act and Assert
        assertEquals(graphDataDto, graphDataDto2);
        int expectedHashCodeResult = graphDataDto.hashCode();
        assertEquals(expectedHashCodeResult, graphDataDto2.hashCode());
    }

    /**
     * Test {@link GraphDataDto#equals(Object)}, and {@link GraphDataDto#hashCode()}.
     * <ul>
     *   <li>When other is same.</li>
     *   <li>Then return equal.</li>
     * </ul>
     * <p>
     * Methods under test:
     * <ul>
     *   <li>{@link GraphDataDto#equals(Object)}
     *   <li>{@link GraphDataDto#hashCode()}
     * </ul>
     */
    @Test
    @DisplayName("Test equals(Object), and hashCode(); when other is same; then return equal")
    @Tag("MaintainedByDiffblue")
    void testEqualsAndHashCode_whenOtherIsSame_thenReturnEqual() {
        // Arrange
        GraphDataDto graphDataDto = new GraphDataDto();

        // Act and Assert
        assertEquals(graphDataDto, graphDataDto);
        int expectedHashCodeResult = graphDataDto.hashCode();
        assertEquals(expectedHashCodeResult, graphDataDto.hashCode());
    }

    /**
     * Test {@link GraphDataDto#equals(Object)}.
     * <ul>
     *   <li>When other is different.</li>
     *   <li>Then return not equal.</li>
     * </ul>
     * <p>
     * Method under test: {@link GraphDataDto#equals(Object)}
     */
    @Test
    @DisplayName("Test equals(Object); when other is different; then return not equal")
    @Tag("MaintainedByDiffblue")
    void testEquals_whenOtherIsDifferent_thenReturnNotEqual() {
        // Arrange, Act and Assert
        assertNotEquals(new GraphDataDto(), 1);
    }

    /**
     * Test {@link GraphDataDto#equals(Object)}.
     * <ul>
     *   <li>When other is {@code null}.</li>
     *   <li>Then return not equal.</li>
     * </ul>
     * <p>
     * Method under test: {@link GraphDataDto#equals(Object)}
     */
    @Test
    @DisplayName("Test equals(Object); when other is 'null'; then return not equal")
    @Tag("MaintainedByDiffblue")
    void testEquals_whenOtherIsNull_thenReturnNotEqual() {
        // Arrange, Act and Assert
        assertNotEquals(new GraphDataDto(), null);
    }

    /**
     * Test {@link GraphDataDto#equals(Object)}.
     * <ul>
     *   <li>When other is wrong type.</li>
     *   <li>Then return not equal.</li>
     * </ul>
     * <p>
     * Method under test: {@link GraphDataDto#equals(Object)}
     */
    @Test
    @DisplayName("Test equals(Object); when other is wrong type; then return not equal")
    @Tag("MaintainedByDiffblue")
    void testEquals_whenOtherIsWrongType_thenReturnNotEqual() {
        // Arrange, Act and Assert
        assertNotEquals(new GraphDataDto(), "Different type to GraphDataDto");
    }

    /**
     * Test getters and setters.
     * <p>
     * Methods under test:
     * <ul>
     *   <li>{@link GraphDataDto#GraphDataDto()}
     *   <li>{@link GraphDataDto#setLinks(List)}
     *   <li>{@link GraphDataDto#setNodes(List)}
     *   <li>{@link GraphDataDto#toString()}
     *   <li>{@link GraphDataDto#getLinks()}
     *   <li>{@link GraphDataDto#getNodes()}
     * </ul>
     */
    @Test
    @DisplayName("Test getters and setters")
    @Tag("MaintainedByDiffblue")
    void testGettersAndSetters() {
        // Arrange and Act
        GraphDataDto actualGraphDataDto = new GraphDataDto();
        actualGraphDataDto.setLinks(null);
        actualGraphDataDto.setNodes(null);
        String actualToStringResult = actualGraphDataDto.toString();
        List<LinkDto> actualLinks = actualGraphDataDto.getLinks();

        // Assert
        assertEquals("GraphDataDto(nodes=null, links=null)", actualToStringResult);
        assertNull(actualLinks);
        assertNull(actualGraphDataDto.getNodes());
    }

    /**
     * Test getters and setters.
     * <ul>
     *   <li>When {@code null}.</li>
     * </ul>
     * <p>
     * Methods under test:
     * <ul>
     *   <li>{@link GraphDataDto#GraphDataDto(List, List)}
     *   <li>{@link GraphDataDto#setLinks(List)}
     *   <li>{@link GraphDataDto#setNodes(List)}
     *   <li>{@link GraphDataDto#toString()}
     *   <li>{@link GraphDataDto#getLinks()}
     *   <li>{@link GraphDataDto#getNodes()}
     * </ul>
     */
    @Test
    @DisplayName("Test getters and setters; when 'null'")
    @Tag("MaintainedByDiffblue")
    void testGettersAndSetters_whenNull() {
        // Arrange and Act
        GraphDataDto actualGraphDataDto = new GraphDataDto(null, null);
        actualGraphDataDto.setLinks(null);
        actualGraphDataDto.setNodes(null);
        String actualToStringResult = actualGraphDataDto.toString();
        List<LinkDto> actualLinks = actualGraphDataDto.getLinks();

        // Assert
        assertEquals("GraphDataDto(nodes=null, links=null)", actualToStringResult);
        assertNull(actualLinks);
        assertNull(actualGraphDataDto.getNodes());
    }
}
