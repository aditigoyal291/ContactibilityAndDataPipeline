package com.neo4j.backend.aspect;

import com.neo4j.backend.service.RateLimiterService;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.aop.aspectj.MethodInvocationProceedingJoinPoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.aot.DisabledInAotMode;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ContextConfiguration(classes = {RateLimitAspect.class})
@ExtendWith(SpringExtension.class)
@DisabledInAotMode
class RateLimitAspectDiffblueTest {
    @Autowired
    private RateLimitAspect rateLimitAspect;

    @MockBean
    private RateLimiterService rateLimiterService;

    /**
     * Test {@link RateLimitAspect#rateLimit(ProceedingJoinPoint)}.
     * <ul>
     *   <li>When {@link MethodInvocationProceedingJoinPoint#MethodInvocationProceedingJoinPoint(ProxyMethodInvocation)} with methodInvocation is {@code null}.</li>
     * </ul>
     * <p>
     * Method under test: {@link RateLimitAspect#rateLimit(ProceedingJoinPoint)}
     */
    @Test
    @DisplayName("Test rateLimit(ProceedingJoinPoint); when MethodInvocationProceedingJoinPoint(ProxyMethodInvocation) with methodInvocation is 'null'")
    @Disabled("TODO: Complete this test")
    @Tag("MaintainedByDiffblue")
    void testRateLimit_whenMethodInvocationProceedingJoinPointWithMethodInvocationIsNull() throws Throwable {
        // TODO: Diffblue Cover was only able to create a partial test for this method:
        //   Reason: No inputs found that don't throw a trivial exception.
        //   Diffblue Cover tried to run the arrange/act section, but the method under
        //   test threw
        //   java.lang.IllegalArgumentException: MethodInvocation must not be null
        //   See https://diff.blue/R013 to resolve this issue.

        // Arrange and Act
        rateLimitAspect.rateLimit(new MethodInvocationProceedingJoinPoint(null));
    }
}
