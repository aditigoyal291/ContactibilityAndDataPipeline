// components/LoanRelationships.js
import React from "react";

const LoanRelationships = ({ loan, graphData }) => {
  if (!loan || !graphData) return null;

  const loanId = loan.id;
  
  // Find related entities
  const primaryApplicants = graphData.nodes.filter(node => 
    node.label === "Person" && 
    graphData.links.some(link => 
      link.target === loanId && 
      link.source === node.id && 
      link.type === "HAS_LOAN"
    )
  );
  
  const coApplicants = graphData.nodes.filter(node => 
    node.label === "Person" && 
    graphData.links.some(link => 
      link.source === loanId && 
      link.target === node.id && 
      link.type === "HAS_COAPPLICANT"
    )
  );
  
  const references = graphData.nodes.filter(node => 
    node.label === "Person" && 
    graphData.links.some(link => 
      link.source === loanId && 
      link.target === node.id && 
      link.type === "HAS_REFERENCE"
    )
  );

  const PersonCard = ({ person, role }) => (
    <div className="bg-gray-700 p-3 rounded mb-2">
      <div className="flex justify-between items-center">
        <span className="font-medium text-white">{person.properties.name || "Unknown"}</span>
        <span className="bg-gray-600 rounded px-2 py-1 text-xs">{role}</span>
      </div>
      {person.properties.pan && (
        <div className="text-gray-300 text-sm mt-1">PAN: {person.properties.pan}</div>
      )}
      {person.properties.phone && (
        <div className="text-gray-300 text-sm">Phone: {person.properties.phone}</div>
      )}
    </div>
  );

  return (
    <div className="mt-4">
      <h3 className="text-xl font-bold mb-4">Loan Relationships</h3>
      
      <div className="mb-4">
        <h4 className="text-lg font-semibold mb-2">Primary Applicants ({primaryApplicants.length})</h4>
        {primaryApplicants.length > 0 ? (
          primaryApplicants.map(person => (
            <PersonCard key={person.id} person={person} role="Primary Applicant" />
          ))
        ) : (
          <p className="text-gray-400">No primary applicants found</p>
        )}
      </div>
      
      <div className="mb-4">
        <h4 className="text-lg font-semibold mb-2">Co-Applicants ({coApplicants.length})</h4>
        {coApplicants.length > 0 ? (
          coApplicants.map(person => (
            <PersonCard key={person.id} person={person} role="Co-Applicant" />
          ))
        ) : (
          <p className="text-gray-400">No co-applicants found</p>
        )}
      </div>
      
      <div className="mb-4">
        <h4 className="text-lg font-semibold mb-2">References ({references.length})</h4>
        {references.length > 0 ? (
          references.map(person => (
            <PersonCard key={person.id} person={person} role="Reference" />
          ))
        ) : (
          <p className="text-gray-400">No references found</p>
        )}
      </div>
    </div>
  );
};

export default LoanRelationships;