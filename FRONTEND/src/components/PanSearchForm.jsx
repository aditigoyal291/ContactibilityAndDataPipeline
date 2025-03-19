import React, { useState, useEffect } from "react";
import { Search } from "lucide-react";
import GraphVisualization from "./GraphVisualization"; // Import the component

const PanSearchForm = () => {
  const [panNumber, setPanNumber] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [graphData, setGraphData] = useState(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [draggedNode, setDraggedNode] = useState(null);

  // Function to handle form submission
  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!panNumber.trim()) {
      setError("Please enter a valid PAN number");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      // Connect to your API endpoint
      const response = await fetch(
        `http://localhost:5000/api/graph/pan/${panNumber}`
      );

      if (!response.ok) {
        if (response.status === 404) {
          throw new Error("No person found with this PAN number");
        }
        throw new Error("Failed to fetch data");
      }

      const data = await response.json();

      // Ensure we have both nodes and relationships
      if (!data.nodes || !data.relationships || data.nodes.length === 0) {
        throw new Error("Invalid graph data received from server");
      }

      console.log("Graph data received:", data);

      // Initialize positions for nodes with good spacing
      const width = 600;
      const height = 500;
      const centerX = width / 2;
      const centerY = height / 2;
      const radius = Math.min(width, height) * 0.4;

      // Arrange nodes in a circle for initial layout
      data.nodes = data.nodes.map((node, index) => {
        const angle = (index / data.nodes.length) * 2 * Math.PI;
        return {
          ...node,
          x: centerX + radius * Math.cos(angle),
          y: centerY + radius * Math.sin(angle),
          vx: 0,
          vy: 0,
        };
      });

      setGraphData(data);
      setSelectedNode(null);

      // Run force layout simulation
      simulateForceLayout(data.nodes, data.relationships);
    } catch (err) {
      console.error("Error fetching data:", err);
      setError(err.message || "An error occurred while fetching data");
      setGraphData(null);
    } finally {
      setLoading(false);
    }
  };

  // Function to simulate force-directed layout
  const simulateForceLayout = (nodes, relationships) => {
    if (!nodes || nodes.length === 0) return;

    const width = 600;
    const height = 500;
    const strength = -300; // Increased repulsion strength
    const linkDistance = 120; // Increased link distance
    const damping = 0.3;
    const iterations = 100;

    // Simulation loop
    let timeout;
    let currentIteration = 0;

    const runIteration = () => {
      if (currentIteration >= iterations) {
        setGraphData((prevData) => ({ ...prevData })); // Trigger final render
        return;
      }

      // Copy nodes for this iteration
      const nodesCopy = [...nodes];

      // Reset forces
      nodesCopy.forEach((node) => {
        node.fx = 0;
        node.fy = 0;
      });

      // Apply repulsive forces between nodes
      for (let i = 0; i < nodesCopy.length; i++) {
        for (let j = i + 1; j < nodesCopy.length; j++) {
          const nodeA = nodesCopy[i];
          const nodeB = nodesCopy[j];
          const dx = nodeB.x - nodeA.x;
          const dy = nodeB.y - nodeA.y;
          const distance = Math.sqrt(dx * dx + dy * dy) || 1;
          const force = strength / (distance * distance);

          nodeA.fx -= (dx * force) / distance;
          nodeA.fy -= (dy * force) / distance;
          nodeB.fx += (dx * force) / distance;
          nodeB.fy += (dy * force) / distance;
        }
      }

      // Apply attractive forces for links
      relationships.forEach((rel) => {
        const source = nodesCopy.find((n) => n.id === rel.source);
        const target = nodesCopy.find((n) => n.id === rel.target);

        if (source && target) {
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const distance = Math.sqrt(dx * dx + dy * dy) || 1;
          const force = (distance - linkDistance) / 8;

          source.fx += (dx * force) / distance;
          source.fy += (dy * force) / distance;
          target.fx -= (dx * force) / distance;
          target.fy -= (dy * force) / distance;
        }
      });

      // Center constraint (keep nodes in view)
      nodesCopy.forEach((node) => {
        const centerForce = 0.01;
        node.fx += (width / 2 - node.x) * centerForce;
        node.fy += (height / 2 - node.y) * centerForce;
      });

      // Update positions
      nodesCopy.forEach((node) => {
        node.vx = node.vx * damping + node.fx;
        node.vy = node.vy * damping + node.fy;
        node.x += node.vx;
        node.y += node.vy;

        // Boundary constraints
        const padding = 50;
        node.x = Math.max(padding, Math.min(width - padding, node.x));
        node.y = Math.max(padding, Math.min(height - padding, node.y));
      });

      // Update nodes in place
      for (let i = 0; i < nodes.length; i++) {
        nodes[i].x = nodesCopy[i].x;
        nodes[i].y = nodesCopy[i].y;
        nodes[i].vx = nodesCopy[i].vx;
        nodes[i].vy = nodesCopy[i].vy;
      }

      // Update iteration and schedule next
      currentIteration++;
      if (currentIteration % 10 === 0) {
        setGraphData((prevData) => ({ ...prevData })); // Trigger render every 10 iterations
      }

      // Schedule next iteration
      timeout = setTimeout(runIteration, 0);
    };

    // Start the simulation
    runIteration();

    // Clean up
    return () => {
      if (timeout) clearTimeout(timeout);
    };
  };

  // Function to handle node selection
  const handleNodeSelect = (node) => {
    setSelectedNode(node);
  };

  // Function to handle node dragging
  const handleNodeDrag = (node, x, y) => {
    // Update the node position
    const updatedNode = { ...node, x, y };

    // Update graph data
    setGraphData((prevData) => {
      const updatedNodes = prevData.nodes.map((n) =>
        n.id === node.id ? updatedNode : n
      );
      return { ...prevData, nodes: updatedNodes };
    });

    setDraggedNode(updatedNode);
  };

  // Function to handle node drag end
  const handleNodeDragEnd = () => {
    setDraggedNode(null);
  };

  return (
    <div className="flex h-screen w-full overflow-hidden">
      {/* Main graph area */}
      <div className="flex-1 relative">
        <div className="absolute top-0 left-0 right-0 p-4 z-10">
          <form onSubmit={handleSubmit} className="flex gap-2">
            <div className="relative flex-grow">
              <input
                type="text"
                value={panNumber}
                onChange={(e) => setPanNumber(e.target.value.toUpperCase())}
                placeholder="Enter PAN Number"
                className="w-full p-2 pl-10 border rounded-md"
                maxLength="10"
              />
              <Search
                className="absolute left-3 top-2.5 text-gray-400"
                size={18}
              />
            </div>
            <button
              type="submit"
              disabled={loading}
              className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-blue-300"
            >
              {loading ? "Loading..." : "Search"}
            </button>
          </form>
        </div>

        <div className="w-full h-full">
          {graphData && (
            <GraphVisualization
              nodes={graphData.nodes}
              relationships={graphData.relationships}
              onNodeSelect={handleNodeSelect}
              onNodeDrag={handleNodeDrag}
              onNodeDragEnd={handleNodeDragEnd}
              selectedNode={selectedNode}
              draggedNode={draggedNode}
            />
          )}
        </div>
      </div>

      {/* Sidebar for node details */}
      {selectedNode && (
        <div className="w-80 h-full bg-gray-800 overflow-y-auto">
          <NodeDetails
            node={selectedNode}
            onClose={() => setSelectedNode(null)}
            graphData={graphData}
          />
        </div>
      )}
    </div>
  );
};

export default PanSearchForm;
