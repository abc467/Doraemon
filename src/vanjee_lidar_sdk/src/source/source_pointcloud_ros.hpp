/*********************************************************************************************************************
Copyright (c) 2023 Vanjee
All rights reserved

By downloading, copying, installing or using the software you agree to this license. If you do not agree to this 
license, do not download, install, copy or use the software.

License Agreement
For Vanjee LiDAR SDK Library
(3-clause BSD License)

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the 
following conditions are met:
 
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following 
disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following 
disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the names of the Vanjee, nor Suteng Innovation Technology, nor the names of other contributors may be used 
to endorse or promote products derived from this software without specific prior written permission.
 
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*********************************************************************************************************************/

#pragma once
#include "source/source.hpp"
#ifdef ROS_FOUND
#include <ros/ros.h>
#include <sensor_msgs/point_cloud2_iterator.h>
namespace vanjee
{
namespace lidar
{
        inline sensor_msgs::PointCloud2 toRosMsg(const LidarPointCloudMsg &vanjee_msg, const std::string &frame_id,
                                                 bool send_by_rows)
        {
            sensor_msgs::PointCloud2 ros_msg;

            int fields = 4;
#ifdef POINT_TYPE_XYZIRT
            fields = 6;
#endif
#ifdef POINT_TYPE_XYZHSV
            fields = 6;
#endif
            ros_msg.fields.clear();
            ros_msg.fields.reserve(fields);

            if (send_by_rows)
            {
                ros_msg.width = vanjee_msg.width;
                ros_msg.height = vanjee_msg.height;
            }
            else
            {
                ros_msg.width = vanjee_msg.height; 
                ros_msg.height = vanjee_msg.width;
            }

            int offset = 0;
            offset = addPointField(ros_msg, "x", 1, sensor_msgs::PointField::FLOAT32, offset);
            offset = addPointField(ros_msg, "y", 1, sensor_msgs::PointField::FLOAT32, offset);
            offset = addPointField(ros_msg, "z", 1, sensor_msgs::PointField::FLOAT32, offset);
#ifdef POINT_TYPE_XYZI
            offset = addPointField(ros_msg, "intensity", 1, sensor_msgs::PointField::FLOAT32, offset);
#endif
#ifdef POINT_TYPE_XYZIRT
            offset = addPointField(ros_msg, "intensity", 1, sensor_msgs::PointField::FLOAT32, offset);          
            offset = addPointField(ros_msg, "ring", 1, sensor_msgs::PointField::UINT16, offset);
            offset = addPointField(ros_msg, "timestamp", 1, sensor_msgs::PointField::FLOAT64, offset);
#endif
#ifdef POINT_TYPE_XYZHSV
            offset = addPointField(ros_msg, "h", 1, sensor_msgs::PointField::FLOAT32, offset);          
            offset = addPointField(ros_msg, "s", 1, sensor_msgs::PointField::FLOAT32, offset);
            offset = addPointField(ros_msg, "v", 1, sensor_msgs::PointField::FLOAT32, offset);
#endif

            ros_msg.point_step = offset;
            ros_msg.row_step = ros_msg.width * ros_msg.point_step;
            ros_msg.is_dense = vanjee_msg.is_dense;
            ros_msg.data.resize(ros_msg.point_step * vanjee_msg.points.size());

            sensor_msgs::PointCloud2Iterator<float> iter_x_(ros_msg, "x");
            sensor_msgs::PointCloud2Iterator<float> iter_y_(ros_msg, "y");
            sensor_msgs::PointCloud2Iterator<float> iter_z_(ros_msg, "z");
#ifdef POINT_TYPE_XYZI
            sensor_msgs::PointCloud2Iterator<float> iter_intensity_(ros_msg, "intensity");
#endif            
#ifdef POINT_TYPE_XYZIRT
            sensor_msgs::PointCloud2Iterator<float> iter_intensity_(ros_msg, "intensity");
            sensor_msgs::PointCloud2Iterator<uint16_t> iter_ring_(ros_msg, "ring");
            sensor_msgs::PointCloud2Iterator<double> iter_timestamp_(ros_msg, "timestamp");
#endif
#ifdef POINT_TYPE_XYZHSV
            sensor_msgs::PointCloud2Iterator<float> iter_h_(ros_msg, "h");
            sensor_msgs::PointCloud2Iterator<float> iter_s_(ros_msg, "s");
            sensor_msgs::PointCloud2Iterator<float> iter_v_(ros_msg, "v");
#endif

            if (send_by_rows)
            {
                for (size_t i = 0; i < vanjee_msg.height; i++)
                {
                    for (size_t j = 0; j < vanjee_msg.width; j++)
                    {
                        const LidarPointCloudMsg::PointT &point = vanjee_msg.points[i + j * vanjee_msg.height];

                        *iter_x_ = point.x;
                        *iter_y_ = point.y;
                        *iter_z_ = point.z;                     

                        ++iter_x_;
                        ++iter_y_;
                        ++iter_z_;
#ifdef POINT_TYPE_XYZI                       
                        *iter_intensity_ = point.intensity;
                        ++iter_intensity_;
#endif

#ifdef POINT_TYPE_XYZIRT
                        *iter_intensity_ = point.intensity;
                        *iter_ring_ = point.ring;
                        *iter_timestamp_ = point.timestamp;

                        ++iter_intensity_;
                        ++iter_ring_;
                        ++iter_timestamp_;
#endif
#ifdef POINT_TYPE_XYZHSV
                        *iter_h_ = point.h;
                        *iter_s_ = point.s;
                        *iter_h_= point.v;

                        ++iter_h_;
                        ++iter_s_;
                        ++iter_v_;
#endif
                    }
                }
            }
            else
            {
                for (size_t i = 0; i < vanjee_msg.points.size(); i++)
                {
                    const LidarPointCloudMsg::PointT &point = vanjee_msg.points[i];

                    *iter_x_ = point.x;
                    *iter_y_ = point.y;
                    *iter_z_ = point.z;

                    ++iter_x_;
                    ++iter_y_;    
                    ++iter_z_;
#ifdef POINT_TYPE_XYZI                   
                    *iter_intensity_ = point.intensity;
                    ++iter_intensity_;
#endif

#ifdef POINT_TYPE_XYZIRT
                    *iter_ring_ = point.ring;
                    *iter_intensity_ = point.intensity;
                    *iter_timestamp_ = point.timestamp;
                    ++iter_intensity_;
                    ++iter_ring_;
                    ++iter_timestamp_;
#endif
#ifdef POINT_TYPE_XYZHSV
                        *iter_h_= point.h;
                        *iter_s_ = point.s;
                        *iter_v_ = point.v;

                        ++iter_h_;
                        ++iter_s_;
                        ++iter_v_;
#endif
                }
            }

            ros_msg.header.seq = vanjee_msg.seq;
            ros_msg.header.stamp = ros_msg.header.stamp.fromSec(vanjee_msg.timestamp);
            ros_msg.header.frame_id = frame_id;

            return ros_msg;
        }
        /// @brief DestinationPointCloudRos publish point clouds under ROS topic '/vanjee_lidar points'
        class DestinationPointCloudRos : public DestinationPointCloud
        {
        private:
            std::shared_ptr<ros::NodeHandle> nh_;
            ros::Publisher pub_;   
            std::string frame_id_; 
            bool send_by_rows_;

        public:
            /// @brief Initialize the 'DestinationPacketRos' instance
            virtual void init(const YAML::Node &config);
            /// @brief Publish point cloud in ROS topic '/vanjee_lidar points'
            virtual void sendPointCloud(const LidarPointCloudMsg &msg);
            virtual ~DestinationPointCloudRos() = default;
        };

        inline void DestinationPointCloudRos::init(const YAML::Node &config)
        {
            yamlRead<bool>(config["ros"], "ros_send_by_rows", send_by_rows_, true);

            bool dense_points;
            yamlRead<bool>(config["driver"], "dense_points", dense_points, false);
            yamlRead<std::string>(config["ros"], "ros_frame_id", frame_id_, "vanjee_lidar");
            std::string ros_send_topic;
            yamlRead<std::string>(config["ros"], "ros_send_point_cloud_topic", ros_send_topic, "vanjee_lidar_points");

            nh_ = std::unique_ptr<ros::NodeHandle>(new ros::NodeHandle());
            pub_ = nh_->advertise<sensor_msgs::PointCloud2>(ros_send_topic, 10);
        }
        inline void DestinationPointCloudRos::sendPointCloud(const LidarPointCloudMsg &msg)
        {
          pub_.publish(toRosMsg(msg, frame_id_, send_by_rows_));
        }
} 

} 

#endif
#ifdef ROS2_FOUND
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/point_cloud2_iterator.hpp>
namespace vanjee
{
namespace lidar
{
        inline sensor_msgs::msg::PointCloud2 toRosMsg(const LidarPointCloudMsg &vanjee_msg, const std::string &frame_id,
                                                 bool send_by_rows)
        {
            sensor_msgs::msg::PointCloud2 ros_msg;

            int fields = 4;
#ifdef POINT_TYPE_XYZIRT
            fields = 6;
#endif
#ifdef POINT_TYPE_XYZHSV
            fields = 6;
#endif
            ros_msg.fields.clear();
            ros_msg.fields.reserve(fields);

            if (send_by_rows)
            {
                ros_msg.width = vanjee_msg.width;
                ros_msg.height = vanjee_msg.height;
            }
            else
            {
                ros_msg.width = vanjee_msg.height; 
                ros_msg.height = vanjee_msg.width;
            }

            int offset = 0;
            offset = addPointField(ros_msg, "x", 1, sensor_msgs::msg::PointField::FLOAT32, offset);
            offset = addPointField(ros_msg, "y", 1, sensor_msgs::msg::PointField::FLOAT32, offset);
            offset = addPointField(ros_msg, "z", 1, sensor_msgs::msg::PointField::FLOAT32, offset);
#ifdef POINT_TYPE_XYZI
            offset = addPointField(ros_msg, "intensity", 1, sensor_msgs::msg::PointField::FLOAT32, offset);
#endif
#ifdef POINT_TYPE_XYZIRT
            offset = addPointField(ros_msg, "intensity", 1, sensor_msgs::msg::PointField::FLOAT32, offset);          
            offset = addPointField(ros_msg, "ring", 1, sensor_msgs::msg::PointField::UINT16, offset);
            offset = addPointField(ros_msg, "timestamp", 1, sensor_msgs::msg::PointField::FLOAT64, offset);
#endif
#ifdef POINT_TYPE_XYZHSV
            offset = addPointField(ros_msg, "h", 1, sensor_msgs::msg::PointField::FLOAT32, offset);          
            offset = addPointField(ros_msg, "s", 1, sensor_msgs::msg::PointField::FLOAT32, offset);
            offset = addPointField(ros_msg, "v", 1, sensor_msgs::msg::PointField::FLOAT32, offset);
#endif

#if 0
  std::cout << "off:" << offset << std::endl;
#endif

            ros_msg.point_step = offset;
            ros_msg.row_step = ros_msg.width * ros_msg.point_step;
            ros_msg.is_dense = vanjee_msg.is_dense;
            ros_msg.data.resize(ros_msg.point_step * vanjee_msg.points.size());

            sensor_msgs::PointCloud2Iterator<float> iter_x_(ros_msg, "x");
            sensor_msgs::PointCloud2Iterator<float> iter_y_(ros_msg, "y");
            sensor_msgs::PointCloud2Iterator<float> iter_z_(ros_msg, "z");
#ifdef POINT_TYPE_XYZI
            sensor_msgs::PointCloud2Iterator<float> iter_intensity_(ros_msg, "intensity");
#endif            
#ifdef POINT_TYPE_XYZIRT
            sensor_msgs::PointCloud2Iterator<float> iter_intensity_(ros_msg, "intensity");
            sensor_msgs::PointCloud2Iterator<uint16_t> iter_ring_(ros_msg, "ring");
            sensor_msgs::PointCloud2Iterator<double> iter_timestamp_(ros_msg, "timestamp");
#endif
#ifdef POINT_TYPE_XYZHSV
            sensor_msgs::PointCloud2Iterator<float> iter_h_(ros_msg, "h");
            sensor_msgs::PointCloud2Iterator<float> iter_s_(ros_msg, "s");
            sensor_msgs::PointCloud2Iterator<float> iter_v_(ros_msg, "v");
#endif

            if (send_by_rows)
            {
                for (size_t i = 0; i < vanjee_msg.height; i++)
                {
                    for (size_t j = 0; j < vanjee_msg.width; j++)
                    {
                        const LidarPointCloudMsg::PointT &point = vanjee_msg.points[i + j * vanjee_msg.height];

                        *iter_x_ = point.x;
                        *iter_y_ = point.y;
                        *iter_z_ = point.z;                     

                        ++iter_x_;
                        ++iter_y_;
                        ++iter_z_;
#ifdef POINT_TYPE_XYZI                       
                        *iter_intensity_ = point.intensity;
                        ++iter_intensity_;
#endif

#ifdef POINT_TYPE_XYZIRT
                        *iter_intensity_ = point.intensity;
                        *iter_ring_ = point.ring;
                        *iter_timestamp_ = point.timestamp;

                        ++iter_intensity_;
                        ++iter_ring_;
                        ++iter_timestamp_;
#endif
#ifdef POINT_TYPE_XYZHSV
                        *iter_h_ = point.h;
                        *iter_s_ = point.s;
                        *iter_h_= point.v;

                        ++iter_h_;
                        ++iter_s_;
                        ++iter_v_;
#endif
                    }
                }
            }
            else
            {
                for (size_t i = 0; i < vanjee_msg.points.size(); i++)
                {
                    const LidarPointCloudMsg::PointT &point = vanjee_msg.points[i];

                    *iter_x_ = point.x;
                    *iter_y_ = point.y;
                    *iter_z_ = point.z;


                    ++iter_x_;
                    ++iter_y_;    
                    ++iter_z_;
#ifdef POINT_TYPE_XYZI                   
                    *iter_intensity_ = point.intensity;
                    ++iter_intensity_;
#endif

#ifdef POINT_TYPE_XYZIRT
                    *iter_ring_ = point.ring;
                    *iter_intensity_ = point.intensity;
                    *iter_timestamp_ = point.timestamp;
                    ++iter_intensity_;
                    ++iter_ring_;
                    ++iter_timestamp_;
#endif
#ifdef POINT_TYPE_XYZHSV
                    *iter_h_= point.h;
                    *iter_s_ = point.s;
                    *iter_v_ = point.v;

                    ++iter_h_;
                    ++iter_s_;
                    ++iter_v_;
#endif
                }
            }

            ros_msg.header.stamp.sec = (uint32_t)floor(vanjee_msg.timestamp);
            ros_msg.header.stamp.nanosec = (uint32_t)round((vanjee_msg.timestamp - ros_msg.header.stamp.sec) * 1e9);
            ros_msg.header.frame_id = frame_id;

            return ros_msg;
        }
        /// @brief DestinationPointCloudRos publish point clouds under ROS topic '/vanjee_lidar points'
        class DestinationPointCloudRos : virtual public DestinationPointCloud
        {
        private:
            std::shared_ptr<rclcpp::Node> node_ptr_;
            rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr pub_;   
            std::string frame_id_; 
            bool send_by_rows_;

        public:
            /// @brief Initialize the 'DestinationPacketRos' instance
            virtual void init(const YAML::Node &config);
            /// @brief Publish point cloud in ROS topic '/vanjee_lidar points'
            virtual void sendPointCloud(const LidarPointCloudMsg &msg);
            virtual ~DestinationPointCloudRos() = default;
        };

        inline void DestinationPointCloudRos::init(const YAML::Node &config)
        {
            yamlRead<bool>(config["ros"], "ros_send_by_rows", send_by_rows_, true);

            bool dense_points;
            yamlRead<bool>(config["driver"], "dense_points", dense_points, false);
            yamlRead<std::string>(config["ros"], "ros_frame_id", frame_id_, "vanjee_lidar");
            std::string ros_send_topic;
            yamlRead<std::string>(config["ros"], "ros_send_point_cloud_topic", ros_send_topic, "vanjee_lidar_points");

            static int node_index = 0;
            std::stringstream node_name;
            node_name << "vanjee_lidar_points_destination_" << node_index++;
            node_ptr_.reset(new rclcpp::Node(node_name.str()));
            pub_ = node_ptr_->create_publisher<sensor_msgs::msg::PointCloud2>(ros_send_topic, 100);
            
        }
        inline void DestinationPointCloudRos::sendPointCloud(const LidarPointCloudMsg &msg)
        {
            pub_->publish(toRosMsg(msg, frame_id_, send_by_rows_));
        }
} 

} 

#endif