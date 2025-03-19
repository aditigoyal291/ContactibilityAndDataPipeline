package com.neo4j.backend.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.aot.DisabledInAotMode;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ContextConfiguration(classes = {RedisConfig.class})
@ExtendWith(SpringExtension.class)
@DisabledInAotMode
class RedisConfigDiffblueTest {
    @Autowired
    private RedisConfig redisConfig;

    @MockBean
    private RedisConnectionFactory redisConnectionFactory;

    /**
     * Test {@link RedisConfig#redisTemplate(RedisConnectionFactory)}.
     * <ul>
     *   <li>Then DefaultSerializer return {@link JdkSerializationRedisSerializer}.</li>
     * </ul>
     * <p>
     * Method under test: {@link RedisConfig#redisTemplate(RedisConnectionFactory)}
     */
    @Test
    @DisplayName("Test redisTemplate(RedisConnectionFactory); then DefaultSerializer return JdkSerializationRedisSerializer")
    @Tag("MaintainedByDiffblue")
    void testRedisTemplate_thenDefaultSerializerReturnJdkSerializationRedisSerializer() {
        // Arrange and Act
        RedisTemplate<String, Object> actualRedisTemplateResult = redisConfig.redisTemplate(new LettuceConnectionFactory());

        // Assert
        RedisSerializer<?> defaultSerializer = actualRedisTemplateResult.getDefaultSerializer();
        assertTrue(defaultSerializer instanceof JdkSerializationRedisSerializer);
        RedisSerializer<?> keySerializer = actualRedisTemplateResult.getKeySerializer();
        assertTrue(keySerializer instanceof StringRedisSerializer);
        RedisSerializer<String> stringSerializer = actualRedisTemplateResult.getStringSerializer();
        assertTrue(stringSerializer instanceof StringRedisSerializer);
        RedisSerializer<?> valueSerializer = actualRedisTemplateResult.getValueSerializer();
        assertTrue(valueSerializer instanceof StringRedisSerializer);
        assertFalse(actualRedisTemplateResult.isExposeConnection());
        assertTrue(actualRedisTemplateResult.isEnableDefaultSerializer());
        Class<Object> expectedTargetType = Object.class;
        assertEquals(expectedTargetType, defaultSerializer.getTargetType());
        Class<String> expectedTargetType2 = String.class;
        Class<?> targetType = keySerializer.getTargetType();
        assertEquals(expectedTargetType2, targetType);
        assertSame(targetType, stringSerializer.getTargetType());
        assertSame(targetType, valueSerializer.getTargetType());
        assertSame(defaultSerializer, actualRedisTemplateResult.getHashKeySerializer());
        assertSame(defaultSerializer, actualRedisTemplateResult.getHashValueSerializer());
    }
}
