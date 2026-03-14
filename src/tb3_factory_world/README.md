tb3_factory_world
================

This package contains a simple Gazebo factory world:
- Outer boundary: 50m (X) x 70m (Y)
- 3 inner rectangular obstacles ("blocks") as provided by the user.

Files:
- worlds/factory_50x70_3blocks.world
- launch/tb3_factory_gazebo.launch

Usage:
  source /opt/ros/noetic/setup.bash
  export TURTLEBOT3_MODEL=burger
  cd ~/catkin_ws && catkin_make
  source ~/catkin_ws/devel/setup.bash
  roslaunch tb3_factory_world tb3_factory_gazebo.launch model:=burger

Then run SLAM:
  roslaunch turtlebot3_slam turtlebot3_slam.launch slam_methods:=gmapping
