// Modified App.js with loan click handling
import React, { useState, useEffect } from "react";

import {
  fetchGraphData,
  fetchNodeByPAN,
  fetchLoanDetails,
} from "../services/Neo4jService.jsx";

import NodeDetails from "./NodeDetails.jsx";

import SearchBar from "./SearchBar.jsx";
import ForceGraph2D from "react-force-graph-2d";

import Navbar from "./Navbar.jsx";

const API_URL = "http://localhost:8080/api";

function Dashboard({ setIsAuthenticated }) {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [selectedNode, setSelectedNode] = useState(null);
  const [selectedCompanies, setSelectedCompanies] = useState([]);
  const [selectedLoan, setSelectedLoan] = useState(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const [userEmail, setUserEmail] = useState(
    localStorage.getItem("userEmail") || ""
  );

  useEffect(() => {
    // Load initial graph data
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    try {
      setLoading(true);
      const data = await fetchGraphData();
      setGraphData(data);
      setLoading(false);
    } catch (err) {
      setError("Failed to load graph data");
      setLoading(false);
    }
  };

  useEffect(() => {
    const storedEmail = localStorage.getItem("userEmail");
    if (storedEmail) {
      setUserEmail(storedEmail);
    }
  });

  const handleSearch = async (pan) => {
    try {
      setLoading(true);
      const data = await fetchNodeByPAN(pan);
      if (data.nodes.length > 0) {
        setGraphData(data);
        // Select the main node (the one searched for)
        setSelectedNode(data.nodes.find((node) => node.properties.pan === pan));
      } else {
        setError("No results found for this PAN number");
      }
      setLoading(false);
    } catch (err) {
      setError("Error searching for PAN number");
      setLoading(false);
    }
  };

  const handleNodeClick = async (node) => {
    if (!node || !node.properties) return; // Prevents crashes

    setSelectedNode(node); // Always set selected node

    if (node.label === "Company") {
      try {
        setLoading(true);
        const response = await fetch(
          `${API_URL}/node/company/${
            node.properties.companyId || node.properties.id
          }`
        );
        const data = await response.json();

        if (!data.nodes || !data.links) {
          throw new Error("Invalid data structure from API");
        }

        const employees = data.nodes.filter((n) => n.label === "Person");

        // Ensure companies are stored safely
        setSelectedCompanies((prevCompanies) => {
          if (prevCompanies.some((c) => c.id === node.id)) {
            return prevCompanies; // Prevent duplicates
          }
          return [...prevCompanies, { id: node.id, employees }];
        });

        setGraphData((prevData) => ({
          nodes: [
            ...(prevData.nodes || []),
            ...data.nodes.filter(
              (n) => !prevData.nodes.some((p) => p.id === n.id)
            ),
          ],
          links: [
            ...(prevData.links || []),
            ...data.links.filter(
              (l) =>
                !prevData.links.some(
                  (p) => p.source === l.source && p.target === l.target
                )
            ),
          ],
        }));

        setLoading(false);
      } catch (error) {
        console.error("Error fetching company employees:", error);
        setError("Failed to fetch company employees");
        setLoading(false);
      }
    } else if (node.label === "Loan") {
      try {
        setLoading(true);

        const response = await fetch(
          `${API_URL}/node/loan/${node.properties.loanId || node.properties.id}`
        );
        const data = await response.json();

        if (!data.nodes || !data.links) {
          throw new Error("Invalid data structure from API");
        }

        // Process the node to format dates properly
        const processedNode = { ...node };

        // Check if applicationDate exists and is in the complex format
        if (
          processedNode.properties.applicationDate &&
          typeof processedNode.properties.applicationDate === "object" &&
          processedNode.properties.applicationDate.year
        ) {
          const appDate = processedNode.properties.applicationDate;
          // Format as ISO string
          processedNode.properties.applicationDate = `${
            appDate.year.low
          }-${String(appDate.month.low).padStart(2, "0")}-${String(
            appDate.day.low
          ).padStart(2, "0")}T${String(appDate.hour.low).padStart(
            2,
            "0"
          )}:${String(appDate.minute.low).padStart(2, "0")}:${String(
            appDate.second.low
          ).padStart(2, "0")}`;
        }

        // Do the same for decisionDate
        if (
          processedNode.properties.decisionDate &&
          typeof processedNode.properties.decisionDate === "object" &&
          processedNode.properties.decisionDate.year
        ) {
          const decDate = processedNode.properties.decisionDate;
          processedNode.properties.decisionDate = `${decDate.year.low}-${String(
            decDate.month.low
          ).padStart(2, "0")}-${String(decDate.day.low).padStart(
            2,
            "0"
          )}T${String(decDate.hour.low).padStart(2, "0")}:${String(
            decDate.minute.low
          ).padStart(2, "0")}:${String(decDate.second.low).padStart(2, "0")}`;
        }

        // Set the processed node as the selected loan
        setSelectedLoan(processedNode);

        // Also process any loan nodes in the returned data
        const processedData = {
          nodes: data.nodes.map((n) => {
            if (n.label === "Loan") {
              const processed = { ...n };

              // Format applicationDate
              if (
                processed.properties.applicationDate &&
                typeof processed.properties.applicationDate === "object" &&
                processed.properties.applicationDate.year
              ) {
                const appDate = processed.properties.applicationDate;
                processed.properties.applicationDate = `${
                  appDate.year.low
                }-${String(appDate.month.low).padStart(2, "0")}-${String(
                  appDate.day.low
                ).padStart(2, "0")}T${String(appDate.hour.low).padStart(
                  2,
                  "0"
                )}:${String(appDate.minute.low).padStart(2, "0")}:${String(
                  appDate.second.low
                ).padStart(2, "0")}`;
              }

              // Format decisionDate
              if (
                processed.properties.decisionDate &&
                typeof processed.properties.decisionDate === "object" &&
                processed.properties.decisionDate.year
              ) {
                const decDate = processed.properties.decisionDate;
                processed.properties.decisionDate = `${
                  decDate.year.low
                }-${String(decDate.month.low).padStart(2, "0")}-${String(
                  decDate.day.low
                ).padStart(2, "0")}T${String(decDate.hour.low).padStart(
                  2,
                  "0"
                )}:${String(decDate.minute.low).padStart(2, "0")}:${String(
                  decDate.second.low
                ).padStart(2, "0")}`;
              }

              return processed;
            }
            return n;
          }),
          links: data.links,
        };

        // Update the graph with the processed data
        setGraphData((prevData) => ({
          nodes: [
            ...(prevData.nodes || []),
            ...processedData.nodes.filter(
              (n) => !prevData.nodes.some((p) => p.id === n.id)
            ),
          ],
          links: [
            ...(prevData.links || []),
            ...processedData.links.filter(
              (l) =>
                !prevData.links.some(
                  (p) => p.source === l.source && p.target === l.target
                )
            ),
          ],
        }));

        setLoading(false);
      } catch (error) {
        console.error("Error fetching loan details:", error);
        setError("Failed to fetch loan details");
        setLoading(false);
      }
    }
  };

  return (
    <>
      <Navbar />
      <div className="min-h-screen overflow-hidden">
        <div className="flex h-screen bg-gray-900 text-white ">
          {/* Left sidebar - Search & Controls */}
          <div className="w-80 bg-gray-800 p-4 flex flex-col">
            <h1 className="text-xl font-bold mb-4">Contactibility</h1>
            <SearchBar onSearch={handleSearch} />

            {loading && <p className="mt-4">Loading...</p>}
            {error && <p className="mt-4 text-red-400">{error}</p>}

           

            <div className="mt-auto">
              <button
                onClick={loadInitialData}
                className="w-full bg-blue-600 hover:bg-blue-700 text-white py-2 px-4 rounded"
              >
                Reset View
              </button>
            </div>
          </div>

          {/* Main content - Graph visualization */}
          <div className="flex-1 relative">
            <ForceGraph2D
              graphData={graphData}
              nodeLabel={(node) => node.label}
              nodeAutoColorBy="label"
              nodeRelSize={8}
              linkDistance={200}
              d3Force="link"
              d3VelocityDecay={0.3}
              linkDirectionalArrowLength={(link) => 5} // Increased arrow length for ALL links
              linkDirectionalArrowRelPos={0.7} // Moved back a bit to make arrows more visible
              linkWidth={(link) => (link.type === "HAS_LOAN" ? 1.5 : 1)}
              linkDirectionalParticles={(link) =>
                link.type === "HAS_LOAN" ? 2 : 1
              }
              linkDirectionalParticleWidth={(link) =>
                link.type === "HAS_LOAN" ? 1.5 : 1
              }
              enableNodeDrag={true}
              cooldownTime={5000}
              linkColor={(link) => {
                if (link.type === "HAS_LOAN") return "#ff6b6b";
                if (link.type === "HAS_COAPPLICANT") return "#48dbfb";
                if (link.type === "HAS_REFERENCE") return "#1dd1a1";
                return "#a5b1c2";
              }}
              onNodeClick={handleNodeClick}
              // Link labels with smaller font
              linkCanvasObjectMode={(link) => "after"}
              linkCanvasObject={(link, ctx, globalScale) => {
                // Only show link labels when zoomed in enough
                if (globalScale < 1.2) return;

                const { source, target } = link;
                const midX = (source.x + target.x) / 2;
                const midY = (source.y + target.y) / 2;
                const label = link.type;

                const fontSize = Math.max(7 / globalScale, 4); // Smaller text
                ctx.font = `${fontSize}px Sans-Serif`;

                // Add background for better readability
                const textWidth = ctx.measureText(label).width;
                ctx.fillStyle = "rgba(0,0,0,0.5)";
                ctx.fillRect(
                  midX - textWidth / 2 - 1,
                  midY - fontSize / 2,
                  textWidth + 2,
                  fontSize + 1
                );

                ctx.fillStyle = "#ffffff"; // White text for contrast
                ctx.textAlign = "center";
                ctx.textBaseline = "middle";
                ctx.fillText(label, midX, midY);
              }}
              // Node rendering with text inside
              nodeCanvasObjectMode={() => "replace"}
              nodeCanvasObject={(node, ctx, globalScale) => {
                const nodeRadius = 10; // Back to original size

                // Draw the node circle first
                ctx.beginPath();
                ctx.arc(node.x, node.y, nodeRadius, 0, 2 * Math.PI, false);

                // Color logic
                let nodeColor = "#ffcc00"; // Default
                if (node.label === "Person") nodeColor = "#d174d8";
                else if (node.label === "Loan") nodeColor = "#f9ca24";
                else if (node.label === "Company") nodeColor = "#60a3bc";

                ctx.fillStyle = node.color || nodeColor;
                ctx.fill();

                // Outline
                ctx.strokeStyle = "#ffffff";
                ctx.lineWidth = 0.8;
                ctx.stroke();

                // Text inside the node
                const fontSize = Math.max(9 / globalScale, 5); // Smaller font
                ctx.font = `${fontSize}px Sans-Serif`;
                ctx.textAlign = "center";
                ctx.textBaseline = "middle";

                let text =
                  node.properties?.name || node.properties?.loanId || node.id;
                const maxWidth = nodeRadius * 1.8; // Constrain text width

                // Truncate if needed
                if (ctx.measureText(text).width > maxWidth) {
                  while (
                    ctx.measureText(text + "..").width > maxWidth &&
                    text.length > 1
                  ) {
                    text = text.slice(0, -1);
                  }
                  text += "..";
                }

                // Draw text inside node
                ctx.fillStyle = "#000000"; // Black text
                ctx.fillText(text, node.x, node.y);
              }}
              // Improved force simulation settings
              d3AlphaDecay={0.015}
              d3AlphaMin={0.001}
              warmupTicks={100}
              cooldownTicks={200}
              repulsivity={30}
              // Additional forces to prevent overlapping
              d3Force1={(d3) => {
                // Add collision force to prevent node overlap
                d3.force("collision", d3.forceCollide(15));

                // Add charge force to make nodes repel each other
                d3.force("charge").strength(-80);

                // Add center force to keep the graph centered
                d3.force("center").strength(0.1);
              }}
            />

            {/* Node details panel - shows when a node is selected */}
            {selectedNode && (
              <NodeDetails
                node={selectedNode}
                onClose={() => setSelectedNode(null)}
              />
            )}
          </div>
        </div>
      </div>
    </>
  );
}

export default Dashboard;
