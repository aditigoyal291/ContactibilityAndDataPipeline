import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Link } from "react-router-dom";

const AuthForm = ({ onLogin, onSignup }) => {
  const navigate = useNavigate();
  const [isLogin, setIsLogin] = useState(true);
  const [formData, setFormData] = useState({
    email: "",
    password: "",
  });
  const [passwordError, setPasswordError] = useState("");
  const [showPassword, setShowPassword] = useState(false);

  const passwordRegex =
    /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$/;
  const handleToggle = () => {
    setIsLogin(!isLogin);
    setPasswordError("");
  };

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData({
      ...formData,
      [name]: value,
    });
    if (name === "password") {
      validatePassword(value);
    }
  };
  const validatePassword = (password) => {

    if (!passwordRegex.test(password)) {
      setPasswordError(
        "Password must be at least 8 characters long, include uppercase, lowercase, a number, and a special character."
      );
    } else {
      setPasswordError("");
    }
  };

  const handleSubmit = (e) => {
    e.preventDefault();

    if (!isLogin && passwordError) {
      return;
    }
    if (isLogin) {
      // Login
      onLogin({
        email: formData.email,
        password: formData.password,
      });
    } else {
      // Signup
      onSignup({
        email: formData.email,
        password: formData.password,
      });
    }
  };

  const togglePasswordVisibility = () => {
    setShowPassword(!showPassword);
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100 px-4 py-6 sm:px-6 sm:py-8">
      <div className="bg-white p-5 sm:p-8 rounded-lg shadow-md w-full max-w-md">
        <div className="flex flex-col sm:flex-row sm:justify-between sm:items-center mb-6 gap-4 sm:gap-0">
          <h2 className="text-xl sm:text-2xl font-bold text-gray-800">
            {isLogin ? "Login" : "Sign Up"}
          </h2>

          <div className="flex items-center justify-center sm:justify-end">
            <span
              className={`mr-2 text-xs sm:text-sm ${
                !isLogin ? "font-medium text-gray-900" : "text-gray-500"
              }`}
            >
              Sign Up
            </span>
            <label className="relative inline-flex items-center cursor-pointer">
              <input
                type="checkbox"
                className="sr-only"
                checked={isLogin}
                onChange={handleToggle}
              />

              <div className="w-10 sm:w-11 h-5 sm:h-6 bg-gray-200 rounded-full peer peer-checked:bg-blue-600">
                <div
                  className={`absolute top-0.5 left-0.5 bg-white w-4 sm:w-5 h-4 sm:h-5 rounded-full transition-all duration-300 ${
                    isLogin ? "translate-x-5" : ""
                  }`}
                ></div>
              </div>
            </label>
            <span
              className={`ml-2 text-xs sm:text-sm ${
                isLogin ? "font-medium text-gray-900" : "text-gray-500"
              }`}
            >
              Login
            </span>
          </div>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="mb-4">
            <label
              className="block text-gray-700 text-xs sm:text-sm font-bold mb-2"
              htmlFor="email"
            >
              Email
            </label>
            <input
              id="email"
              name="email"
              type="email"
              className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500"
              placeholder="your@email.com"
              value={formData.email}
              onChange={handleChange}
              required
            />
          </div>

          <div className="mb-4">
            <label
              className="block text-gray-700 text-xs sm:text-sm font-bold mb-2"
              htmlFor="password"
            >
              Password
            </label>
            <div className="relative">
              <input
                id="password"
                name="password"
                type={showPassword ? "text" : "password"}
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                placeholder="********"
                value={formData.password}
                onChange={handleChange}
                required
              />
              <button
                type="button"
                className="absolute inset-y-0 right-0 pr-3 flex items-center text-gray-500 hover:text-gray-700"
                onClick={togglePasswordVisibility}
              >
                {showPassword ? (
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    width="16"
                    height="16"
                    fill="currentColor"
                    viewBox="0 0 16 16"
                  >
                    <path d="M13.359 11.238C15.064 9.856 16 8 16 8s-3-5.5-8-5.5S0 8 0 8s.939 1.856 2.641 3.238l-.708.708C.942 10.732 0 9.075 0 8s3-6.5 8-6.5S16 6.925 16 8s-.942 2.732-2.929 3.945l-.708-.708zM8 11a3 3 0 1 0 0-6 3 3 0 0 0 0 6" />
                    <path d="M7 5.5a.5.5 0 1 1-1 0 .5.5 0 0 1 1 0m1.5 0a.5.5 0 1 1 1 0 .5.5 0 0 1-1 0m-5 0a.5.5 0 1 1 1 0 .5.5 0 0 1-1 0m8 0a.5.5 0 1 1 1 0 .5.5 0 0 1-1 0m-4 4a.5.5 0 1 1 1 0 .5.5 0 0 1-1 0m5 0a.5.5 0 1 1 1 0 .5.5 0 0 1-1 0" />
                  </svg>
                ) : (
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    width="16"
                    height="16"
                    fill="currentColor"
                    viewBox="0 0 16 16"
                  >
                    <path d="M10.79 12.912l-1.614-1.615a3.5 3.5 0 0 1-4.474-4.474l-2.06-2.06C.938 6.278 0 8 0 8s3 5.5 8 5.5a7.029 7.029 0 0 0 2.79-.588zM5.21 3.088A7.028 7.028 0 0 1 8 2.5c5 0 8 5.5 8 5.5s-.939 1.721-2.641 3.238l-2.062-2.062a3.5 3.5 0 0 0-4.474-4.474L5.21 3.089z" />
                    <path d="M5.525 7.646a2.5 2.5 0 0 0 2.829 2.829l-2.83-2.829zm4.95.708l-2.829-2.83a2.5 2.5 0 0 1 2.829 2.829zm3.171 6l-12-12 .708-.708 12 12-.708.708z" />
                  </svg>
                )}
              </button>
            </div>
          </div>

          <div className="mb-6">
            <button
              type="submit"
              className="w-full bg-blue-500 hover:bg-blue-600 text-white font-bold py-2 px-4 text-sm sm:text-base rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-opacity-50 transition-all duration-300"
            >
              {isLogin ? "Log In" : "Sign Up"}
            </button>
          </div>

          {/* Add this after the password input field */}
          {!isLogin && (
            <div className="mt-1">
              <div className="text-xs text-gray-600 mb-1">
                Password strength:
              </div>
              <div className="h-1 w-full bg-gray-200 rounded-full">
                <div
                  className={`h-1 rounded-full ${
                    formData.password.length === 0
                      ? "w-0"
                      : formData.password.length < 8
                      ? "w-1/4 bg-red-500"
                      : !passwordRegex.test(formData.password)
                      ? "w-2/4 bg-yellow-500"
                      : "w-full bg-green-500"
                  }`}
                ></div>
              </div>
              {passwordError && (
                <p className="text-red-500 text-xs mt-1">{passwordError}</p>
              )}
            </div>
          )}
        </form>
      </div>
    </div>
  );
};

export default AuthForm;
