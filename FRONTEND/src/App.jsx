//App.jsx

import React, { useState, useEffect } from "react";
import Dashboard from "./components/Dashboard";
import {
  BrowserRouter as Router,
  Route,
  Routes,
  Navigate,
} from "react-router-dom";
import AuthForm from "./components/AuthForm";
import ProtectedRoute from "./components/ProtectedRoute";
import TasksComponent from "./components/TaskComponent";
import ResetPassword from "./components/ResetPassword";
import ForgotPassword from "./components/ForgotPassword";

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [userData, setUserData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);

  // Check authentication status on initial load
  useEffect(() => {
    const checkAuth = () => {
      const token = localStorage.getItem("token");
      // Check if token exists AND is valid
      if (token) {
        // You could add JWT token validation here
        setIsAuthenticated(true);
        setUserData({ email: localStorage.getItem("userEmail") });
      } else {
        // Clear any leftover data if token doesn't exist
        localStorage.removeItem("userEmail");
        setIsAuthenticated(false);
      }
      setIsLoading(false);
    };

    checkAuth();

    // Listen for storage changes (for multi-tab support)
    const handleStorageChange = (e) => {
      if (e.key === "token") {
        checkAuth();
      }
    };

    window.addEventListener("storage", handleStorageChange);
    return () => window.removeEventListener("storage", handleStorageChange);
  }, []);

  const handleLogin = async ({ email, password }) => {
    try {
      const response = await fetch("http://localhost:8080/user/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });

      const data = await response.json();
      if (response.ok) {
        localStorage.setItem("token", data.token);
        localStorage.setItem("userEmail", email);
        setIsAuthenticated(true);
        setUserData({ email });
        // Use navigate instead of window.location for better React Router integration
        // If you're using older code, this is fine
        window.location.href = "/";
      } else {
        alert(data.message || "Invalid credentials");
      }
    } catch (error) {
      console.error("Login error:", error);
      alert("An error occurred. Please try again.");
    }
  };

  const handleSignup = async (userData) => {
    try {
      const response = await fetch("http://localhost:8080/user/signup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(userData),
      });

      const data = await response.json();

      if (response.ok) {
        alert("Successfully signed up! Logging you in...");
        localStorage.setItem("token", data.token);
        setIsAuthenticated(true);
        setUserData({ email: userData.email });
      } else if (response.status === 409) {
        alert("User already exists!");
      } else {
        alert(data.message || "Signup failed");
      }
    } catch (error) {
      console.error("Signup error:", error);
      alert("An error occurred. Please try again.");
    }
  };

  const handleLogout = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("userEmail");
    setIsAuthenticated(false);
    setUserData(null);
  };

  // Show loading state while checking authentication
  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-screen">
        Loading...
      </div>
    );
  }

  return (
    <Router>
      <Routes>
        <Route
          path="/"
          element={
            isAuthenticated ? (
              <Dashboard
                setIsAuthenticated={setIsAuthenticated}
                onLogout={handleLogout}
              />
            ) : (
              <AuthForm onLogin={handleLogin} onSignup={handleSignup} />
            )
          }
        />

        <Route
          path="/dashboard/*"
          element={
            <ProtectedRoute isAuthenticated={isAuthenticated}>
              <Dashboard
                setIsAuthenticated={setIsAuthenticated}
                onLogout={handleLogout}
              />
            </ProtectedRoute>
          }
        />
        <Route
          path="/dashboard/task"
          element={
            <ProtectedRoute isAuthenticated={isAuthenticated}>
              <TasksComponent />
            </ProtectedRoute>
          }
        />

        <Route path="/reset-password" element={<ResetPassword />} />
        <Route path="/forgot-password" element={<ForgotPassword />} />
        <Route
          path="/login"
          element={
            isAuthenticated ? (
              <Navigate to="/" />
            ) : (
              <AuthForm onLogin={handleLogin} onSignup={handleSignup} />
            )
          }
        />
      </Routes>
    </Router>
  );
}

export default App;
