import React, { useRef, useState, useEffect, useCallback } from "react";

const GraphVisualization = ({
  nodes,
  relationships,
  width = "100%",
  height = "100vh",
  onNodeSelect,
  onNodeDrag,
  onNodeDragEnd,
  selectedNode,
  draggedNode,
}) => {
  const svgRef = useRef(null);
  const [dragging, setDragging] = useState(null);
  const onNodeDragRef = useRef(onNodeDrag);
  const onNodeDragEndRef = useRef(onNodeDragEnd);

  // Update refs when props change
  useEffect(() => {
    onNodeDragRef.current = onNodeDrag;
  }, [onNodeDrag]);

  useEffect(() => {
    onNodeDragEndRef.current = onNodeDragEnd;
  }, [onNodeDragEnd]);

  // Handle mouse down for dragging
  const handleMouseDown = useCallback((node, e) => {
    e.preventDefault();
    setDragging(node);
  }, []);

  const handleMouseMove = useCallback(
    (e) => {
      if (dragging && svgRef.current) {
        const svg = svgRef.current;
        const rect = svg.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        if (onNodeDragRef.current) onNodeDragRef.current(dragging, x, y);
      }
    },
    [dragging]
  );

  const handleMouseUp = useCallback(() => {
    if (onNodeDragEndRef.current && dragging)
      onNodeDragEndRef.current(dragging);
    setDragging(null);
  }, [dragging]);

  // Dragging effect
  useEffect(() => {
    if (dragging) {
      window.addEventListener("mousemove", handleMouseMove);
      window.addEventListener("mouseup", handleMouseUp);
      return () => {
        window.removeEventListener("mousemove", handleMouseMove);
        window.removeEventListener("mouseup", handleMouseUp);
      };
    }
  }, [dragging, handleMouseMove, handleMouseUp]);

  // Node styling functions
  const getNodeColor = (node) => {
    const typeColors = {
      Person: "#60A5FA",
      Loan: "#F59E0B",
      Company: "#10B981",
      BankAccount: "#FBBF24",
      Property: "#EC4899",
      Vehicle: "#8B5CF6",
    };
    return typeColors[node.type] || "#9CA3AF";
  };

  const getNodeLabel = (node) => {
    if (node.type === "Loan") {
      return node.properties?.loanId || node.properties?.loanNumber || "Loan";
    }
    return node.properties?.name || node.label || node.id || "Unknown";
  };

  const getNodeSubLabel = (node) => node.type || node.label || "Node";

  if (!nodes || !relationships) {
    return (
      <div className="text-gray-500 text-center py-10">
        No graph data available
      </div>
    );
  }

  return (
    <div className="bg-white p-4 rounded-lg shadow-md overflow-hidden">
      <svg
        width={width}
        height={height}
        className="mx-auto border border-gray-200 rounded"
        ref={svgRef}
      >
        {/* Relationship lines */}
        {relationships.map((rel, index) => {
          const source = nodes.find((n) => n.id === rel.source);
          const target = nodes.find((n) => n.id === rel.target);
          if (!source || !target) return null;

          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const labelPosX = source.x + dx * 0.6;
          const labelPosY = source.y + dy * 0.6;

          return (
            <g key={`rel-${index}`}>
              <line
                x1={source.x}
                y1={source.y}
                x2={target.x}
                y2={target.y}
                stroke="#4B5563"
                strokeWidth="2"
                strokeOpacity="0.8"
                markerEnd="url(#arrowhead)"
              />
              <rect
                x={labelPosX - 30}
                y={labelPosY - 10}
                width="60"
                height="20"
                fill="white"
                opacity="0.9"
                rx="5"
              />
              <text
                x={labelPosX}
                y={labelPosY + 5}
                fill="#4B5563"
                fontSize="10"
                textAnchor="middle"
                fontWeight="500"
              >
                {rel.type}
              </text>
            </g>
          );
        })}

        {/* Arrow marker definition */}
        <defs>
          <marker
            id="arrowhead"
            markerWidth="6"
            markerHeight="6"
            refX="6"
            refY="3"
            orient="auto"
          >
            <polygon points="0 0, 6 3, 0 6" fill="#4B5563" />
          </marker>
        </defs>

        {/* Nodes */}
        {nodes.map((node) => {
          const nodeRadius = 35;
          const nodeColor = getNodeColor(node);
          const nodeLabel = getNodeLabel(node);
          const nodeSubLabel = getNodeSubLabel(node);

          return (
            <g
              key={`node-${node.id}`}
              onMouseDown={(e) => handleMouseDown(node, e)}
              className="cursor-grab active:cursor-grabbing"
            >
              <circle
                cx={node.x}
                cy={node.y}
                r={nodeRadius}
                fill={nodeColor}
                stroke={selectedNode?.id === node.id ? "#2563EB" : "none"}
                strokeWidth="2"
              />
              <text
                x={node.x}
                y={node.y - 5}
                fill="white"
                fontSize="12"
                fontWeight="bold"
                textAnchor="middle"
                dominantBaseline="middle"
              >
                {`nodeLabel.length > 12 ? ${nodeLabel.substring(
                  0,
                  10
                )}... : nodeLabel`}
              </text>
              <text
                x={node.x}
                y={node.y + 12}
                fill="white"
                fontSize="10"
                textAnchor="middle"
                dominantBaseline="middle"
                opacity="0.9"
              >
                {nodeSubLabel}
              </text>
            </g>
          );
        })}
      </svg>

      {/* Legend */}
      <div className="flex flex-wrap justify-center gap-4 mt-4 p-2 bg-gray-50 rounded">
        {[
          "Person",
          "Company",
          "Loan",
          "Bank Account",
          "Property",
          "Vehicle",
        ].map((type) => (
          <div key={type} className="flex items-center">
            <div
              className="w-4 h-4 rounded-full mr-2"
              style={{ backgroundColor: getNodeColor({ type }) }}
            />
            <span className="text-xs">{type}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

export default GraphVisualization;
