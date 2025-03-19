import React from "react";

const NodeDetails = ({ node, onClose }) => {
  console.log("node data recieived", node);
  if (!node) return null;

  // Function to format property values
  const formatValue = (key, value) => {
    // Skip _id field
    if (key === "_id") {
      return null;
    }

    // Handle string dates
    if (key.toLowerCase().includes("date") && typeof value === "string") {
      return new Date(value).toLocaleDateString();
    }

    // Handle amounts
    if (key.toLowerCase().includes("amount") && typeof value === "number") {
      return `₹${value.toLocaleString()}`;
    }

    // For any other object that might cause problems, convert to JSON string
    if (value && typeof value === "object") {
      try {
        return JSON.stringify(value);
      } catch (error) {
        return "Complex Object";
      }
    }

    return value;
  };

  // Function to get title based on node type
  const getTitle = () => {
    if (node.label === "Person" || node.label === "Person") {
      return node.properties.name || "Person Details";
    } else if (node.label === "Loan" || node.label === "Loan") {
      return `Loan #${node.properties.loanId || node.id}`;
    } else if (node.label === "Company" || node.label === "Company") {
      return node.properties.name || "Company Details";
    }
    return `${node.label || node.type} Details`;
  };

  // Function to get color based on node type - only keeping this for Person nodes
  const getColor = () => {
    if (node.label === "Person") return "bg-purple-500";
    if (node.label === "Loan") return "bg-yellow-500";
    if (node.label === "Company") return "bg-blue-500";
    return "bg-gray-500";
  };

  return (
    <div className="fixed top-20 right-4 w-80 md:w-80 bg-gray-800 rounded-lg shadow-lg overflow-hidden z-10">
      <div
        className={`p-3 ${getColor()} text-white flex justify-between items-center`}
      >
        <h3 className="font-bold">{getTitle()}</h3>
        <button onClick={onClose} className="text-white hover:text-gray-300">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="20"
            height="20"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <line x1="18" y1="6" x2="6" y2="18"></line>
            <line x1="6" y1="6" x2="18" y2="18"></line>
          </svg>
        </button>
      </div>

      <div className="p-4 overflow-y-auto flex-1">
        <div className="p-4 h-[55rem] overflow-x-hidden">
          {/* Show node type */}
          <div className="mb-2">
            <span className="bg-gray-700 text-xs px-2 py-1 rounded text-gray-300">
              {node.label}
            </span>
          </div>

          {/* Show node properties */}
          <div className="space-y-2">
            {Object.entries(node.properties || {}).map(([key, value]) => {
              // Skip _id field and roles field completely
              if (key === "_id" || key === "roles") return null;

              const formattedValue = formatValue(key, value);
              if (formattedValue === null) return null;

              return (
                <div key={key} className="flex justify-between">
                  <span className="text-gray-400">{key}:</span>
                  <span className="text-white font-medium">
                    {formattedValue}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};

export default NodeDetails;
