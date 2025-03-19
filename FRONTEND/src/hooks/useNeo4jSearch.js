import { useState } from "react";
import neo4jService from "../services/Neo4jService";

/**
 * Custom hook for handling Neo4j database searches
 */
const useNeo4jSearch = () => {
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  /**
   * Search for customer by PAN number
   * @param {string} panNumber - The PAN number to search for
   */
  const searchByPan = async (panNumber) => {
    if (!panNumber) {
      setError("PAN number is required");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const data = await neo4jService.queryByPan(panNumber);
      setResults(data);
    } catch (err) {
      setError(err.response?.data?.error || "Failed to fetch customer data");
      setResults(null);
    } finally {
      setLoading(false);
    }
  };

  return {
    results,
    loading,
    error,
    searchByPan,
  };
};

export default useNeo4jSearch;
