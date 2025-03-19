package com.neo4j.backend.service;


import com.neo4j.backend.dto.GraphDataDto;

public interface GraphService {
    GraphDataDto getFullGraph();
    GraphDataDto getPersonByPan(String panNumber);
    GraphDataDto getCompanyById(String companyId);
    GraphDataDto getLoanById(String loanId);
}
