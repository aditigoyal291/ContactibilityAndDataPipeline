// services/neo4jService.js
import neo4j from "neo4j-driver";

const API_URL = "http://localhost:8080/api";
// Create Neo4j driver instance (use environment variables in production)
const driver = neo4j.driver(
  "bolt://localhost:7687",
  neo4j.auth.basic("neo4j", "password") // Replace with your actual credentials
);

// Convert Neo4j records to a format suitable for the graph visualization
// Modified processGraphData function to handle null values
// Modified server-side processGraphData function
const processGraphData = (records) => {
  const nodes = [];
  const links = [];
  const nodeMap = new Map();

  records.forEach((record) => {
    record.forEach((value) => {
      // Skip null values
      if (value === null) {
        return;
      }

      if (value.segments) {
        // Process paths
        value.segments.forEach((segment) => {
          processNode(segment.start, nodeMap, nodes);
          processNode(segment.end, nodeMap, nodes);
          processRelationship(segment.relationship, nodeMap, links);
        });
      } else if (value.labels) {
        // Process individual nodes
        processNode(value, nodeMap, nodes);
      } else if (value.type) {
        // Process individual relationships
        processRelationship(value, nodeMap, links);
      }
    });
  });

  return { nodes, links };
};

// No changes needed to these functions

const processNode = (node, nodeMap, nodes) => {
  if (!nodeMap.has(node.identity.low)) {
    const processedNode = {
      id: node.identity.low.toString(),
      label: node.labels[0],
      properties: Object.fromEntries(
        Object.entries(node.properties).map(([key, value]) => {
          // Handle Neo4j integers
          if (value !== null && typeof value === "object" && "low" in value) {
            return [key, value.low];
          }
          return [key, value];
        })
      ),
    };

    nodes.push(processedNode);
    nodeMap.set(node.identity.low, processedNode);
  }
};

// Add this function to your services/neo4jService.js file
export const fetchLoanDetails = async (loanId) => {
  console.log(`Fetching loan details for ID: ${loanId}`);
  try {
    const url = `${API_URL}/node/loan/${loanId}`;
    console.log(`Making request to: ${url}`);

    const response = await fetch(url);
    console.log("Response status:", response.status);

    if (!response.ok) {
      const errorBody = await response.text();
      console.error("Error response body:", errorBody);
      throw new Error(
        `Network response was not ok (${response.status}): ${errorBody}`
      );
    }

    const data = await response.json();
    console.log("Received loan data:", data);
    return data;
  } catch (error) {
    console.error("Detailed error fetching loan details:", error);
    throw error;
  }
};

const processRelationship = (rel, nodeMap, links) => {
  links.push({
    id: rel.identity.low.toString(),
    source: rel.start.low.toString(),
    target: rel.end.low.toString(),
    type: rel.type,
    properties: Object.fromEntries(
      Object.entries(rel.properties).map(([key, value]) => {
        if (value !== null && typeof value === "object" && "low" in value) {
          return [key, value.low];
        }
        return [key, value];
      })
    ),
  });
};

export const fetchGraphData = async () => {
  try {
    const response = await fetch(`${API_URL}/graph`);
    if (!response.ok) {
      throw new Error("Network response was not ok");
    }
    return await response.json();
  } catch (error) {
    console.error("Error fetching graph data:", error);
    throw error;
  }
};

// Fetch node by PAN number and its relationships
// export const fetchNodeByPAN = async (panNumber) => {
//   try {
//     const response = await fetch(`${API_URL}/node/pan/${panNumber}`);
//     if (!response.ok) {
//       throw new Error("Network response was not ok");
//     }
//     return await response.json();
//   } catch (error) {
//     console.error("Error fetching node by PAN:", error);
//     throw error;
//   }
// };
export const fetchNodeByPAN = async (pan) => {
  console.log(`Fetching node data for PAN: ${pan}`);
  try {
    const url = `${API_URL}/node/pan/${pan}`;
    console.log(`Making request to: ${url}`);

    const response = await fetch(url);
    console.log("Response status:", response.status);

    if (!response.ok) {
      // Try to get more details about the error
      const errorBody = await response.text();
      console.error("Error response body:", errorBody);
      throw new Error(
        `Network response was not ok (${response.status}): ${errorBody}`
      );
    }

    const data = await response.json();
    console.log("Received data:", data);
    return data;
  } catch (error) {
    console.error("Detailed error fetching node by PAN:", error);
    throw error;
  }
};
export const fetchCompanyEmployees = async (companyId) => {
  try {
    const response = await fetch(`${API_URL}/node/company/${companyId}`);

    if (!response.ok) {
      throw new Error("Network response was not ok");
    }

    return await response.json();
  } catch (error) {
    console.error("Error fetching company employees:", error);
    throw error;
  }
};

export default driver;
