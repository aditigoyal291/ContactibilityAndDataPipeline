package com.neo4j.backend.controller;



import com.neo4j.backend.annotation.RateLimit;
import com.neo4j.backend.dto.GraphDataDto;
import com.neo4j.backend.service.GraphService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
public class GraphController {
    private final GraphService graphService;

    @RateLimit(limit = 5, seconds = 60)
    @GetMapping("/graph")
    public ResponseEntity<GraphDataDto> getFullGraph() {
        log.info("Received request to fetch full graph");
        return ResponseEntity.ok(graphService.getFullGraph());
    }

    @RateLimit(limit = 10, seconds = 60)
    @GetMapping("/node/pan/{panNumber}")
    public ResponseEntity<GraphDataDto> getPersonByPan(@PathVariable String panNumber) {
        log.info("Received request for PAN: {}", panNumber);
        return ResponseEntity.ok(graphService.getPersonByPan(panNumber));
    }

    @RateLimit(limit = 10, seconds = 60)
    @GetMapping("/node/company/{companyId}")
    public ResponseEntity<GraphDataDto> getCompanyById(@PathVariable String companyId) {
        log.info("Received request for company ID: {}", companyId);
        return ResponseEntity.ok(graphService.getCompanyById(companyId));
    }

    @RateLimit(limit = 10, seconds = 60)
    @GetMapping("/node/loan/{loanId}")
    public ResponseEntity<GraphDataDto> getLoanById(@PathVariable String loanId) {
        log.info("Received request for loan ID: {}", loanId);
        return ResponseEntity.ok(graphService.getLoanById(loanId));
    }
}