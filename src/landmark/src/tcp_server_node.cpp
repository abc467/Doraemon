#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <ros/ros.h>
#include <geometry_msgs/Quaternion.h>
#include <tf2/LinearMath/Quaternion.h>

#include <cartographer_ros_msgs/LandmarkList.h>
#include <cartographer_ros_msgs/LandmarkEntry.h>

int PORT;
int BUFFER_SIZE;
std::string SERVER_IP;
double translation_weight;
double rotation_weight;
ros::Publisher qrPosePub;

void handleReceivedData(const std::vector<uint8_t> &receivedData)
{
    if (receivedData.size() < 21)
    {
        ROS_ERROR("Received data length is too short for parsing.");
        return;
    }

    uint8_t result = receivedData[0];
    double x_offset, y_offset, yaw;
    int tag_number;
    for (size_t i = 1; i <= 19; i++)
    {
        result ^= receivedData[i];
    }

    bool on_qrcode = (receivedData[1] & 0x40) == 0x40;
    unsigned int x_position = (receivedData[2] * 0x80 * 0x4000) + (receivedData[3] * 0x4000) + (receivedData[4] * 0x80) + receivedData[5];
    if (x_position > 0x800000)
    {
        x_position = (0x1000000 - x_position);
        x_offset = -static_cast<double>(x_position) / 10000;
    }
    else
    {
        x_offset = static_cast<double>(x_position) / 10000;
    }
    unsigned int y_position = (receivedData[6] * 0x80) + receivedData[7];
    if (y_position > 0x2000)
    {
        y_position = (0x4000 - y_position);
        y_offset = -static_cast<double>(y_position) / 10000;
    }
    else
    {
        y_offset = static_cast<double>(y_position) / 10000;
    }

    unsigned int angle = (receivedData[10] * 0x80) + receivedData[11];
    yaw = static_cast<double>(angle) / 10;
    if (yaw > 180.0)
    {
        yaw = yaw - 360;
    }
    yaw = yaw * (M_PI / 180.0);

    int raw_tag_number = (receivedData[14] * 0x80 * 0x4000) + (receivedData[15] * 0x4000) + (receivedData[16] * 0x80) + receivedData[17];
    if (raw_tag_number < 0x3FFF)
    {
        raw_tag_number = (receivedData[16] * 0x80) + receivedData[17];
    }
    tag_number = static_cast<int>(raw_tag_number);

    if (result == receivedData[20])
    {
        if (on_qrcode)
        {
            geometry_msgs::Quaternion orientation;
            orientation.x = 0;
            orientation.y = 0;
            orientation.z = std::sin(yaw / 2.0);
            orientation.w = std::cos(yaw / 2.0);

            try
            {
                cartographer_ros_msgs::LandmarkList qrPoseList;
                cartographer_ros_msgs::LandmarkEntry qrPoseMsg;
                qrPoseMsg.id = "landmark2" + std::to_string(tag_number);
                qrPoseMsg.translation_weight = translation_weight;
                qrPoseMsg.rotation_weight = rotation_weight;
                qrPoseMsg.tracking_from_landmark_transform.position.x = x_offset;
                qrPoseMsg.tracking_from_landmark_transform.position.y = y_offset;
                qrPoseMsg.tracking_from_landmark_transform.position.z = 0.0;
                qrPoseMsg.tracking_from_landmark_transform.orientation = orientation;
                qrPoseList.landmarks.push_back(qrPoseMsg);
                qrPosePub.publish(qrPoseList);
            }
            catch (const ros::serialization::StreamOverrunException &e)
            {
                ROS_ERROR("ros::serialization::StreamOverrunException: %s", e.what());
            }
        }
    }
}

int main(int argc, char **argv)
{
    ros::init(argc, argv, "DM_Reader");  // 保留你的节点名字
    ros::NodeHandle nh;

    nh.param<int>("/DM_Reader/PORT", PORT, 3004);
    nh.param<int>("/DM_Reader/BUFFER_SIZE", BUFFER_SIZE, 1024);
    nh.param<std::string>("/DM_Reader/SERVER_IP", SERVER_IP, "10.168.38.11");
    nh.param<double>("/DM_Reader/translation_weight", translation_weight, 50);
    nh.param<double>("/DM_Reader/rotation_weight", rotation_weight, 50);

    qrPosePub = nh.advertise<cartographer_ros_msgs::LandmarkList>("qr_pose", 10);

    while (ros::ok())
    {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0)
        {
            ROS_ERROR("Socket creation error");
            sleep(1);
            continue;
        }

        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(PORT);

        if (inet_pton(AF_INET, SERVER_IP.c_str(), &serv_addr.sin_addr) <= 0)
        {
            ROS_ERROR("Invalid address/ Address not supported");
            close(sock);
            sleep(1);
            continue;
        }

        if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        {
            ROS_ERROR("Connection Failed, retrying...");
            close(sock);
            sleep(1);
            continue;
        }

        ROS_INFO("Connected to DM server: %s:%d", SERVER_IP.c_str(), PORT);

        while (ros::ok())
        {
            uint8_t buffer[21];
            ssize_t bytesRead = recv(sock, reinterpret_cast<char *>(buffer), sizeof(buffer), 0);
            if (bytesRead <= 0)
            {
                ROS_WARN("Connection lost, reconnecting...");
                close(sock);
                break;
            }

            std::vector<uint8_t> receivedData(buffer, buffer + bytesRead);
            handleReceivedData(receivedData);
        }
    }

    return 0;
}

