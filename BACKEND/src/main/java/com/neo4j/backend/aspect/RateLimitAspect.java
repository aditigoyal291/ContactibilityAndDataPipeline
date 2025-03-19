package com.neo4j.backend.aspect;

import com.neo4j.backend.annotation.RateLimit;
import com.neo4j.backend.service.RateLimiterService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;


@Aspect
@Component
@RequiredArgsConstructor
public class RateLimitAspect {

    private final RateLimiterService rateLimiterService;

    @Around("@annotation(com.neo4j.backend.annotation.RateLimit)")
    public Object rateLimit(ProceedingJoinPoint joinPoint) throws Throwable {
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        String ipAddress = request.getRemoteAddr();

        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        RateLimit rateLimit = method.getAnnotation(RateLimit.class);

        String key = "rate_limit:" + ipAddress + ":" + method.getName();
        boolean allowed = rateLimiterService.tryAcquire(key, rateLimit.limit(), rateLimit.seconds());

        if (allowed) {
            return joinPoint.proceed();
        } else {
            Map<String, String> response = new HashMap<>();
            response.put("message", "Rate limit exceeded. Try again later.");
            return new ResponseEntity<>(response, HttpStatus.TOO_MANY_REQUESTS);
        }
    }
}
