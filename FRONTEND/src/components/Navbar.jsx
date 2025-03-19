import React, { useState } from "react";
import { LogOut } from "lucide-react";

const Navbar = () => {
  const [mobileMenuOpen, setMobileMenuOpen] = React.useState(false);
  const [userEmail, setUserEmail] = useState(
    localStorage.getItem("userEmail") || ""
  );
  const [isOpen, setIsOpen] = useState(false);
  const handleLogout = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("userEmail"); //  Remove userEmail
  
    setUserEmail("");
    window.location.href = "/"; //  Redirect to login page
  };
  return (
    <header className="bg-gray-700 py-4 px-4 sm:px-6 flex flex-wrap justify-between items-center">
      <div className="flex items-center">
        <div className="text-blue-500 font-bold text-2xl sm:text-3xl flex items-center">
          <span className="text-blue-500 mr-2">
            <svg
              width="30"
              height="30"
              viewBox="0 0 24 24"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
            >
              <path
                d="M12 2C6.48 2 2 6.48 2 12C2 17.52 6.48 22 12 22C17.52 22 22 17.52 22 12C22 6.48 17.52 2 12 2Z"
                fill="#2563EB"
              />
              <path d="M8 14L12 18L16 14" stroke="white" strokeWidth="2" />
              <path d="M12 6V14" stroke="white" strokeWidth="2" />
            </svg>
          </span>
          LOANS24
        </div>
      </div>

      {/* Mobile menu button */}
      <button
        className="md:hidden ml-2"
        onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="h-6 w-6 text-gray-700"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d={
              mobileMenuOpen
                ? "M6 18L18 6M6 6l12 12"
                : "M4 6h16M4 12h16M4 18h16"
            }
          />
        </svg>
      </button>

      {/* Desktop navigation */}
      <div
        className={`md:flex items-center ${
          mobileMenuOpen ? "flex w-full mt-4" : "hidden"
        } md:w-auto md:mt-0`}
      >
        <div
          className="flex items-center gap-2 px-4 py-2 rounded-lg hover:cursor-pointer"
          onClick={handleLogout}
        >
          <LogOut className="w-5 h-5 sm:w-6 sm:h-6 text-gray-400" />
          <span className="text-gray-400 font-medium">Logout</span>
        </div>
        <div className="relative inline-block">
          {/* Trigger: User Avatar + Name */}
          <div
            className="flex items-center cursor-pointer"
            onMouseEnter={() => setIsOpen(true)}
            onMouseLeave={() => setIsOpen(false)}
            onClick={() => setIsOpen(!isOpen)} // For touch devices
          >
            <div className="bg-blue-800 text-white rounded-full w-8 h-8 sm:w-10 sm:h-10 flex items-center justify-center font-bold">
              {userEmail ? userEmail[0].toUpperCase() : "G"}
            </div>
            <span className="ml-2 text-gray-700">
              {userEmail ? userEmail.split("@")[0] : "Guest"}
            </span>
          </div>

          {/* Dropdown Menu */}
          <div className="relative">
            {isOpen && (
              <div
                className="absolute left-0 mt-2 w-48 bg-white border border-gray-200 rounded-lg shadow-lg p-3 z-20 -translate-x-1/2"
                onMouseEnter={() => setIsOpen(true)}
                onMouseLeave={() => setIsOpen(false)}
              >
                <p className="text-gray-900 font-semibold">
                  {userEmail ? userEmail.split("@")[0] : "Guest"}
                </p>
              </div>
            )}
          </div>
        </div>
      </div>
    </header>
  );
};

export default Navbar;
