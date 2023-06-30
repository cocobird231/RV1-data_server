cmake_minimum_required(VERSION 3.5)
project(cpp_dataserver)

# Default to C99
if(NOT CMAKE_C_STANDARD)
  set(CMAKE_C_STANDARD 99)
endif()

# Default to C++14
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 14)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

if($ENV{ROS_DISTRO} STREQUAL "foxy")
  add_compile_options(-DROS_DISTRO=1)
elseif($ENV{ROS_DISTRO} STREQUAL "humble")
  add_compile_options(-DROS_DISTRO=2)
else()# eloquent
  add_compile_options(-DROS_DISTRO=0)
endif()

# find dependencies
SET(ENABLE_TS_NODE False CACHE BOOL "Some user-specified option")
if( ENABLE_TS_NODE ) 
  add_compile_options(-DTS_NODE)
endif()

SET(ENABLE_QOS_NODE False CACHE BOOL "Some user-specified option")
if( ENABLE_QOS_NODE ) 
  add_compile_options(-DQOS_NODE)
endif()

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(vehicle_interfaces REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(OpenCV ${VERSION_REQ_OCV} REQUIRED)

include_directories(${OpenCV_INCLUDE_DIRS})

link_directories(${OpenCV_LIBRARY_DIRS})

## Build
add_executable(sub src/main.cpp)
ament_target_dependencies(sub rclcpp vehicle_interfaces sensor_msgs)
target_link_libraries(sub
  ${OpenCV_LIBRARIES}
)

## Install
install(TARGETS
  sub
  DESTINATION lib/${PROJECT_NAME}
)

install(
  DIRECTORY launch
  DESTINATION share/${PROJECT_NAME}
)

ament_package()
