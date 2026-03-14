cmake_minimum_required(VERSION 3.0.2)
project(my_docking_controller)

# 1. 查找依赖包
find_package(catkin REQUIRED COMPONENTS
  roscpp
  tf
  geometry_msgs
  std_msgs
  actionlib           # [新增] Action 库
  actionlib_msgs      # [新增] Action 消息类型
  message_generation  # [新增] 用于生成 Action 消息代码
)

# 2. 添加 Action 文件
add_action_files(
  DIRECTORY action
  FILES AutoDocking.action
)

# 3. 生成消息 (依赖)
generate_messages(
  DEPENDENCIES
  geometry_msgs
  std_msgs
  actionlib_msgs
)

# 4. 声明 Catkin 包依赖
catkin_package(
  INCLUDE_DIRS include
  LIBRARIES smooth_control_law_lib
  CATKIN_DEPENDS 
    roscpp 
    tf 
    geometry_msgs 
    std_msgs 
    actionlib        # [新增]
    actionlib_msgs   # [新增]
    message_runtime  # [新增]
)

# 5. 包含头文件目录
include_directories(
  include
  ${catkin_INCLUDE_DIRS}
)

# 6. 编译核心算法库
add_library(smooth_control_law_lib
  src/smooth_control_law.cpp
)
add_dependencies(smooth_control_law_lib ${${PROJECT_NAME}_EXPORTED_TARGETS} ${catkin_EXPORTED_TARGETS})

# 7. 编译 Action Server 节点
add_executable(docking_tracker_node
  src/docking_tracker_node.cpp
)

# 8. 链接库
target_link_libraries(smooth_control_law_lib
  ${catkin_LIBRARIES}
)

target_link_libraries(docking_tracker_node
  smooth_control_law_lib
  ${catkin_LIBRARIES}
)

# 9. 确保在编译节点前生成了 Action 消息头文件
add_dependencies(docking_tracker_node ${${PROJECT_NAME}_EXPORTED_TARGETS} ${catkin_EXPORTED_TARGETS})