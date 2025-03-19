import React, { useState, useEffect } from "react";
import {
  AlertCircle,
  Calendar,
  Check,
  CheckCircle,
  Clock,
  Plus,
  Trash2,
  X,
} from "lucide-react";
import { useLocation, useNavigate } from "react-router-dom";
import { LogOut } from "lucide-react";

const TasksComponent = ({ setIsAuthenticated }) => {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [userEmail, setUserEmail] = useState(
    localStorage.getItem("userEmail") || ""
  );
  const [isOpen, setIsOpen] = useState(false);

  const handleLogout = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("userEmail"); //  Remove userEmail
    setIsAuthenticated(false); // Ensure the UI updates
    setUserEmail("");
    window.location.href = "/"; //  Redirect to login page
  };

  const location = useLocation();
  const navigate = useNavigate();

  useEffect(() => {
    // Redirect if the user tries to access this page directly
    if (!location.state?.fromParent) {
      navigate("/dashboard"); // Redirect to home or some secure page
    }
  }, [location, navigate]); //this prevents url based redirect fron the dasboard

  const handleBack = () => {
    navigate("/dashboard"); // Navigate to the dashboard page
  };

  const [tasks, setTasks] = useState(() => {
    const savedTasks = localStorage.getItem("tasks");
    return savedTasks
      ? JSON.parse(savedTasks)
      : [
          {
            id: 1,
            title: "Follow up with 5 high priority accounts",
            completed: false,
            priority: "high",
            deadline: "2025-03-20",
          },
          {
            id: 2,
            title: "Submit weekly collection report",
            completed: false,
            priority: "medium",
            deadline: "2025-03-18",
          },
          {
            id: 3,
            title: "Call 8 customers for payment reminders",
            completed: false,
            priority: "high",
            deadline: "2025-03-17",
          },
          {
            id: 4,
            title: "Update status for 12 accounts",
            completed: false,
            priority: "low",
            deadline: "2025-03-25",
          },
        ];
  });

  const [showAddTask, setShowAddTask] = useState(false);
  const [newTask, setNewTask] = useState({
    title: "",
    priority: "medium",
    deadline: "",
  });

  useEffect(() => {
    localStorage.setItem("tasks", JSON.stringify(tasks));
  }, [tasks]);

  const toggleTaskCompletion = (id) => {
    setTasks(
      tasks.map((task) =>
        task.id === id ? { ...task, completed: !task.completed } : task
      )
    );
  };

  const deleteTask = (id) => {
    setTasks(tasks.filter((task) => task.id !== id));
  };

  const addTask = () => {
    if (newTask.title.trim() === "") return;

    const task = {
      id: Date.now(),
      title: newTask.title,
      completed: false,
      priority: newTask.priority,
      deadline: newTask.deadline,
    };

    setTasks([...tasks, task]);
    setNewTask({
      title: "",
      priority: "medium",
      deadline: "",
    });
    setShowAddTask(false);
  };

  // const addTask = async () => {
  //   if (!newTask.title.trim()) return;

  //   const userId = loggedInUser.id; // Ensure you have the logged-in user's ID
  //   const updatedTasks = [...tasks, newTask];

  //   try {
  //     const response = await fetch(`users/${userId}/tasks`, {
  //       method: "PUT",
  //       headers: {
  //         "Content-Type": "application/json",
  //       },
  //       body: JSON.stringify({ tasks: updatedTasks }),
  //     });

  //     if (response.ok) {
  //       setTasks(updatedTasks);
  //       setNewTask({ title: "", priority: "low", deadline: "" }); // Reset input fields
  //       setShowAddTask(false);
  //     } else {
  //       console.error("Failed to add task");
  //     }
  //   } catch (error) {
  //     console.error("Error adding task:", error);
  //   }
  // };

  const priorityColors = {
    high: "bg-red-100 text-red-800",
    medium: "bg-yellow-100 text-yellow-800",
    low: "bg-green-100 text-green-800",
  };

  const isOverdue = (deadline) => {
    if (!deadline) return false;
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const taskDeadline = new Date(deadline);
    return taskDeadline < today;
  };

  const isApproaching = (deadline) => {
    if (!deadline) return false;
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const taskDeadline = new Date(deadline);
    const diffTime = taskDeadline - today;
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    return diffDays >= 0 && diffDays <= 2;
  };

  const formatDate = (dateString) => {
    if (!dateString) return "";
    const date = new Date(dateString);
    return date.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  };

  // Calculate if we have any reminders to show
  const overdueCount = tasks.filter(
    (task) => !task.completed && isOverdue(task.deadline)
  ).length;
  const approachingCount = tasks.filter(
    (task) =>
      !task.completed &&
      !isOverdue(task.deadline) &&
      isApproaching(task.deadline)
  ).length;
  const showReminders = overdueCount > 0 || approachingCount > 0;

  return (
    <>
      <header className="bg-blue-50 py-4 px-4 sm:px-6 flex flex-wrap justify-between items-center">
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
          <div className="text-gray-500 text-xs ml-2">BY CMS</div>
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
            <LogOut className="w-5 h-5 sm:w-6 sm:h-6 text-gray-700" />
            <span className="text-gray-700 font-medium">Logout</span>
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
                  className="absolute left-0 mt-2 w-48 bg-white border border-gray-200 rounded-lg shadow-lg p-3 z-10 -translate-x-1/2"
                  onMouseEnter={() => setIsOpen(true)}
                  onMouseLeave={() => setIsOpen(false)}
                >
                  <p className="text-gray-900 font-semibold">
                    {userEmail ? userEmail.split("@")[0] : "Guest"}
                  </p>
                  <p className="text-gray-600 text-sm">Age: 30</p>
                  <p className="text-gray-600 text-sm">
                    userEmail: {userEmail || "Not Provided"}
                  </p>
                  <p className="text-gray-600 text-sm">Location: New York</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </header>
      <div className="py-8"></div>
      <div className="max-w-4xl mx-auto p-6 bg-white rounded-lg shadow-lg">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold text-blue-800">My Tasks</h1>
          <div className="flex gap-2">
            <button
              onClick={handleBack}
              className="bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-md transition-all"
            >
              Back
            </button>
            <button
              onClick={() => setShowAddTask(!showAddTask)}
              className="flex items-center gap-1 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-md transition-all"
            >
              {showAddTask ? <X size={18} /> : <Plus size={18} />}
              {showAddTask ? "Cancel" : "Add Task"}
            </button>
          </div>
        </div>

        {/* Reminders Section */}
        {showReminders && (
          <div className="mb-6 bg-blue-50 p-4 rounded-md border border-blue-200">
            <div className="flex items-center gap-2 text-blue-800 font-medium mb-2">
              <AlertCircle size={18} />
              <h2>Reminders</h2>
            </div>
            <ul className="space-y-2 pl-6">
              {overdueCount > 0 && (
                <li className="text-red-600 flex items-center gap-2">
                  <Clock size={16} />
                  You have {overdueCount} overdue{" "}
                  {overdueCount === 1 ? "task" : "tasks"}
                </li>
              )}
              {approachingCount > 0 && (
                <li className="text-orange-600 flex items-center gap-2">
                  <Calendar size={16} />
                  {approachingCount} {approachingCount === 1 ? "task" : "tasks"}{" "}
                  due within the next 2 days
                </li>
              )}
            </ul>
          </div>
        )}

        {/* Add Task Form */}
        {showAddTask && (
          <div className="mb-6 p-4 border border-gray-200 rounded-md bg-gray-50">
            <h2 className="text-lg font-medium mb-3 text-gray-700">
              Add New Task
            </h2>
            <div className="space-y-4">
              <div>
                <label
                  htmlFor="title"
                  className="block text-sm font-medium text-gray-700 mb-1"
                >
                  Task Title
                </label>
                <input
                  type="text"
                  id="title"
                  className="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  value={newTask.title}
                  onChange={(e) =>
                    setNewTask({ ...newTask, title: e.target.value })
                  }
                  placeholder="Enter task title"
                />
              </div>

              <div className="flex gap-4">
                <div className="flex-1">
                  <label
                    htmlFor="priority"
                    className="block text-sm font-medium text-gray-700 mb-1"
                  >
                    Priority
                  </label>
                  <select
                    id="priority"
                    className="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    value={newTask.priority}
                    onChange={(e) =>
                      setNewTask({ ...newTask, priority: e.target.value })
                    }
                  >
                    <option value="low">Low</option>
                    <option value="medium">Medium</option>
                    <option value="high">High</option>
                  </select>
                </div>

                <div className="flex-1">
                  <label
                    htmlFor="deadline"
                    className="block text-sm font-medium text-gray-700 mb-1"
                  >
                    Deadline
                  </label>
                  <input
                    type="date"
                    id="deadline"
                    className="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    value={newTask.deadline}
                    onChange={(e) =>
                      setNewTask({ ...newTask, deadline: e.target.value })
                    }
                  />
                </div>
              </div>

              <div className="flex justify-end">
                <button
                  onClick={addTask}
                  className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-md transition-all"
                  disabled={!newTask.title.trim()}
                >
                  Add Task
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Tasks List */}
        <div className="space-y-1">
          {tasks.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              No tasks yet. Add your first task to get started!
            </div>
          ) : (
            tasks.map((task) => {
              const isTaskOverdue = !task.completed && isOverdue(task.deadline);
              const isTaskApproaching =
                !task.completed &&
                !isOverdue(task.deadline) &&
                isApproaching(task.deadline);

              return (
                <div
                  key={task.id}
                  className={`p-4 border ${
                    task.completed
                      ? "bg-gray-50 border-gray-200"
                      : "bg-white border-gray-200"
                  } 
                ${
                  isTaskOverdue
                    ? "border-l-4 border-l-red-500"
                    : isTaskApproaching
                    ? "border-l-4 border-l-orange-500"
                    : ""
                } 
                rounded-md mb-2 flex items-start justify-between transition-all hover:shadow-md`}
                >
                  <div className="flex items-start gap-3">
                    <button
                      onClick={() => toggleTaskCompletion(task.id)}
                      className={`mt-1 flex-shrink-0 rounded-full p-1 
                    ${
                      task.completed
                        ? "text-green-600 bg-green-100"
                        : "text-gray-400 hover:text-gray-600"
                    }`}
                    >
                      {task.completed ? (
                        <CheckCircle size={20} />
                      ) : (
                        <Check size={20} />
                      )}
                    </button>

                    <div className="flex-grow">
                      <p
                        className={`font-medium ${
                          task.completed
                            ? "text-gray-500 line-through"
                            : "text-gray-800"
                        }`}
                      >
                        {task.title}
                      </p>

                      <div className="flex flex-wrap gap-2 mt-2">
                        <span
                          className={`text-xs px-2 py-1 rounded-full ${
                            priorityColors[task.priority]
                          }`}
                        >
                          {task.priority.charAt(0).toUpperCase() +
                            task.priority.slice(1)}
                        </span>

                        {task.deadline && (
                          <span
                            className={`text-xs px-2 py-1 rounded-full flex items-center gap-1
                          ${
                            isTaskOverdue
                              ? "bg-red-100 text-red-800"
                              : isTaskApproaching
                              ? "bg-orange-100 text-orange-800"
                              : "bg-blue-100 text-blue-800"
                          }`}
                          >
                            <Calendar size={12} />
                            {formatDate(task.deadline)}
                            {isTaskOverdue && " (Overdue)"}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>

                  <button
                    onClick={() => deleteTask(task.id)}
                    className="text-gray-400 hover:text-red-600 p-1 transition-colors"
                  >
                    <Trash2 size={18} />
                  </button>
                </div>
              );
            })
          )}
        </div>
      </div>
    </>
  );
};

export default TasksComponent;
