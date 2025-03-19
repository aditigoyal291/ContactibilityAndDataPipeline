import { Navigate } from "react-router-dom";
import { jwtDecode } from "jwt-decode";

const ProtectedRoute = ({ children, isAuthenticated }) => {
  // Use the isAuthenticated prop first if provided
  if (isAuthenticated === false) {
    return <Navigate to="/login" replace />;
  }

  // Fallback to token check if isAuthenticated prop wasn't provided
  const token = localStorage.getItem("token");

  if (!token) {
    return <Navigate to="/login" replace />;
  }

  try {
    const decodedToken = jwtDecode(token);
    const isExpired = decodedToken.exp * 1000 < Date.now(); // Convert to milliseconds

    if (isExpired) {
      localStorage.removeItem("token"); // Clear expired token
      return <Navigate to="/login" replace />;
    }

    return children;
  } catch (error) {
    localStorage.removeItem("token"); // If token is malformed, remove it
    return <Navigate to="/login" replace />;
  }
};

export default ProtectedRoute;
