#ifndef VISUAL_SLAM_HPP
#define VISUAL_SLAM_HPP

#include "webserver.hpp"

class jsonmaker
{
public:
    jsonmaker(/* args */) = default;
    ~jsonmaker() = default;

    std::string json_make(std::string &&type, std::string &data)
    {
        std::string result;
        result += "{\"type\":\"" + type + "\",\"data\":{" + data + "}}";
        return result;
    }

    void json_push_function(std::string &s, std::string &&fn_name)
    {
        s += "\"func\":\"" + fn_name + "\",";
    }

    void json_push_string(std::string &s, std::string &&key, std::string &val)
    {
        s += "\"" + key + "\":\"" + val + "\",";
    }

    void json_push_int(std::string &s, std::string &&key, int val)
    {
        s += "\"" + key + "\":" + std::to_string(val) + ",";
    }

    void json_push_color(std::string &s, std::string &&key, unsigned int val)
    {
        s += "\"" + key + "\":" + std::to_string(val) + ",";
    }

    void json_push_vec3(std::string &s, std::string &&key, float x, float y,
                        float z)
    {
        s += "\"" + key + "\":[" + std::to_string(x) + "," + std::to_string(y) +
             "," + std::to_string(x) + "],";
    }

    void json_push_vec4(std::string &s, std::string &&key, float w, float x,
                        float y, float z)
    {
        s += "\"" + key + "\":[" + std::to_string(w) + "," + std::to_string(x) +
             "," + std::to_string(y) + "," + std::to_string(x) + "],";
    }

    void json_push_float(std::string &s, std::string &&key, float val)
    {
        s += "\"" + key + "\":" + std::to_string(val) + ",";
    }

    void json_push_float_array(std::string &s, std::string &&key,
                               std::vector<float> &vals)
    {
        if (vals.empty())
        {
            return;
        }

        s += "\"" + key + "\":[";
        for (auto i = vals.cbegin(); i != vals.cend(); i++)
        {
            s += std::to_string(*i) + ",";
        }
        s.pop_back();
        s += "],";
    }

    template <typename NumberType>
    void json_push_number_array(std::string &s, std::string &&key,
                                std::vector<NumberType> &vals)
    {
        if (vals.empty())
        {
            return;
        }

        s += "\"" + key + "\":[";
        for (auto i = vals.cbegin(); i != vals.cend(); i++)
        {
            s += std::to_string(*i) + ",";
        }
        s.pop_back();
        s += "],";
    }

    void json_finish(std::string &s) { s.pop_back(); }
};

class visualweb_interface
{
public:
    typedef struct
    {
        float r;
        float g;
        float b;
    } color;

    typedef struct
    {
        float w;
        float x;
        float y;
        float z;
    } vec4;

    typedef struct
    {
        float x;
        float y;
        float z;
    } vec3;

    typedef struct
    {
        float x;
        float y;
    } vec2;

    // variables
    jsonmaker json_maker;
    cyber::webserver server;
    std::string slam_root;
    bool connected;

    visualweb_interface() = default;
    ~visualweb_interface() = default;

    void ws_open()
    {
        this->connected = true;
        websocket_connection_init();
        std::cout << "ws_open" << std::endl;
    }

    void ws_close()
    {
        this->connected = false;
        std::cout << "ws_close" << std::endl;
    }

    void ws_message(std::string &&msg)
    {
        std::cout << "ws_msg" << msg << std::endl;
    }

    void start_visualweb()
    {
        this->connected = false;
        char *root = getenv("SLAM_ROOT");
        if (root == 0)
        {
            std::cout << "Cannot find env `SLAM_ROOT`, eg. /home/user/cartographer" << std::endl;
            _exit(-1);
        }
        this->slam_root = std::string(root);
        this->server.set_root(this->slam_root + "/html");
        bind_ws();
        bind_service();
        this->server.start(28256, 3, 2);
    }
    bool ok() { return this->connected; }

    // ---------------------------------------Figure--------------------------------------
    void figure_clear()
    {
        std::string data;
        json_maker.json_push_function(data, "figure_clear");
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }

    void figure_delete(std::string &&name)
    {
        std::string data;
        json_maker.json_push_function(data, "figure_delete");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    // ------------------------------------------Cloud----------------------------------------
    void cloud_create(std::string &&name, float point_size)
    {
        std::string data;
        json_maker.json_push_function(data, "cloud_create");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_push_float(data, "size", point_size);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }

    void cloud_switch(std::string &&name)
    {
        std::string data;
        json_maker.json_push_function(data, "cloud_switch");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }

    void cloud_push(std::vector<float> points, color color)
    {
        std::string data;
        json_maker.json_push_function(data, "cloud_push");
        json_maker.json_push_float_array(data, "points", points);
        json_maker.json_push_float(data, "r", color.r);
        json_maker.json_push_float(data, "g", color.g);
        json_maker.json_push_float(data, "b", color.b);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }

    void cloud_push_color(std::vector<float> points, std::vector<float> colors)
    {
        std::string data;
        json_maker.json_push_function(data, "cloud_push");
        json_maker.json_push_float_array(data, "points", points);
        json_maker.json_push_float_array(data, "colors", colors);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }

    void cloud_push_one(vec3 position, color color)
    {
        std::string data;
        json_maker.json_push_function(data, "cloud_push_one");
        json_maker.json_push_float(data, "x", position.x);
        json_maker.json_push_float(data, "y", position.y);
        json_maker.json_push_float(data, "z", position.z);
        json_maker.json_push_float(data, "r", color.r);
        json_maker.json_push_float(data, "g", color.g);
        json_maker.json_push_float(data, "b", color.b);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    // ------------------------------------------Scatter----------------------------------------
    void scatter_create(std::string &&name, color color, float size)
    {
        std::string data;
        json_maker.json_push_function(data, "scatter_create");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_push_float(data, "r", color.r);
        json_maker.json_push_float(data, "g", color.g);
        json_maker.json_push_float(data, "b", color.b);
        json_maker.json_push_float(data, "size", size);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void scatter_switch(std::string &&name)
    {
        std::string data;
        json_maker.json_push_function(data, "scatter_switch");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void scatter(std::vector<float> points)
    {
        std::string data;
        json_maker.json_push_function(data, "scatter");
        json_maker.json_push_float_array(data, "points", points);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    // ------------------------------------------Plot----------------------------------------
    void plot_create(std::string &&name, color color)
    {
        std::string data;
        json_maker.json_push_function(data, "plot_create");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_push_float(data, "r", color.r);
        json_maker.json_push_float(data, "g", color.g);
        json_maker.json_push_float(data, "b", color.b);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void plot_switch(std::string &&name)
    {
        std::string data;
        json_maker.json_push_function(data, "plot_switch");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void plot(std::vector<float> points)
    {
        std::string data;
        json_maker.json_push_function(data, "plot");
        json_maker.json_push_float_array(data, "points", points);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    // ------------------------------------------Wave----------------------------------------
    void wave_create(std::string &&name, int count, float xdiv, color color)
    {
        std::string data;
        json_maker.json_push_function(data, "wave_create");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_push_int(data, "count", count);
        json_maker.json_push_float(data, "xdiv", xdiv);
        json_maker.json_push_float(data, "r", color.r);
        json_maker.json_push_float(data, "g", color.g);
        json_maker.json_push_float(data, "b", color.b);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void wave_switch(std::string &&name)
    {
        std::string data;
        json_maker.json_push_function(data, "wave_switch");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void wave_push(std::vector<float> y)
    {
        std::string data;
        json_maker.json_push_function(data, "wave_push");
        json_maker.json_push_float_array(data, "y", y);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    // ------------------------------------------PoseDisplayer----------------------------------------
    void pose_displayer_create(std::string &&name)
    {
        std::string data;
        json_maker.json_push_function(data, "pose_displayer_create");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void pose_displayer_switch(std::string &&name)
    {
        std::string data;
        json_maker.json_push_function(data, "pose_displayer_switch");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void pose_displayer_set_left_hand(float w, float x, float y, float z)
    {
        /**
         * +z  +x
         *  |  /
         *  | /
         *  |/_______+y
         */
        std::string data;
        json_maker.json_push_function(data, "pose_displayer_set_left_hand");
        json_maker.json_push_float(data, "w", w);
        json_maker.json_push_float(data, "x", x);
        json_maker.json_push_float(data, "y", y);
        json_maker.json_push_float(data, "z", z);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void pose_displayer_set_right_hand(float w, float x, float y, float z)
    {
        /**
         *          +z  +x
         *           |  /
         *           | /
         * +y_______ |/
         */
        std::string data;
        json_maker.json_push_function(data, "pose_displayer_set_right_hand");
        json_maker.json_push_float(data, "w", w);
        json_maker.json_push_float(data, "x", x);
        json_maker.json_push_float(data, "y", y);
        json_maker.json_push_float(data, "z", z);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    // ------------------------------------------OccupancyMap----------------------------------------
    void occupancy_map_create(std::string &&name, float resolution)
    {
        std::string data;
        json_maker.json_push_function(data, "occupancy_map_create");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_push_float(data, "resolution", resolution);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void occupancy_map_switch(std::string &&name)
    {
        std::string data;
        json_maker.json_push_function(data, "occupancy_map_switch");
        json_maker.json_push_string(data, "name", name);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }
    void occupancy_map_set(vec2 position, int val)
    {
        if (val > 255)
        {
            val = 255;
        }
        else if (val < 0)
        {
            val = 0;
        }

        std::string data;
        json_maker.json_push_function(data, "occupancy_map_set");
        json_maker.json_push_float(data, "x", position.x);
        json_maker.json_push_float(data, "y", position.y);
        json_maker.json_push_int(data, "val", val);
        json_maker.json_finish(data);
        this->ws->write(json_maker.json_make("call", data));
    }

    // ----------------------------WebSocket----------------------------
    std::shared_ptr<cyber::websocket> ws;
    std::shared_ptr<cyber::websocket> ws_slam_result;
    std::shared_ptr<cyber::websocket> ws_trajectory_list;
    std::shared_ptr<cyber::websocket> ws_submap_query;
    std::shared_ptr<cyber::websocket> ws_control;
    std::shared_ptr<cyber::websocket> ws_landmark;

    virtual void ws_slam_result_onmsg(std::string &&msg) = 0;
    virtual void ws_trajectory_list_onmsg(std::string &&msg) = 0;
    virtual void ws_submap_query_onmsg(std::string &&msg) = 0;
    void bind_ws()
    {
        this->ws = server.web_socket("/figure");
        this->ws->set_onopen(std::bind(&visualweb_interface::ws_open, this));
        this->ws->set_onclose(std::bind(&visualweb_interface::ws_close, this));
        this->ws->set_onmessage(std::bind(&visualweb_interface::ws_message, this, std::placeholders::_1));

        this->ws_slam_result = server.web_socket("/slam_result");
        this->ws_slam_result->set_binary(true);
        this->ws_slam_result->set_onmessage(std::bind(&visualweb_interface::ws_slam_result_onmsg, this, std::placeholders::_1));

        this->ws_trajectory_list = server.web_socket("/trajectory_list");
        this->ws_trajectory_list->set_binary(true);
        this->ws_trajectory_list->set_onmessage(std::bind(&visualweb_interface::ws_trajectory_list_onmsg, this, std::placeholders::_1));

        this->ws_submap_query = server.web_socket("/submap_query");
        this->ws_submap_query->set_binary(true);
        this->ws_submap_query->set_onmessage(std::bind(&visualweb_interface::ws_submap_query_onmsg, this, std::placeholders::_1));

    }

    // -------------------------------------Service----------------------------------
    virtual void websocket_connection_init() = 0;

    virtual void service_reboot(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    virtual void service_get_trajectory_state(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    virtual void service_load_config(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    virtual void service_load_state(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    virtual void service_save_state(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    virtual void service_get_dirlist(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    virtual void service_add_trajectory(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    virtual void service_finish_trajectory(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    virtual void service_finish(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    virtual void service_optimize(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    virtual void service_try_global_relocate(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res) = 0;

    void bind_service()
    {
        this->server.post("/reboot", std::bind(&visualweb_interface::service_reboot,
                                               this, std::placeholders::_1, std::placeholders::_2));

        this->server.get("/trajectory_state", std::bind(&visualweb_interface::service_get_trajectory_state,
                                                        this, std::placeholders::_1, std::placeholders::_2));

        this->server.post("/load_config", std::bind(&visualweb_interface::service_load_config,
                                                    this, std::placeholders::_1, std::placeholders::_2));

        this->server.post("/load_state", std::bind(&visualweb_interface::service_load_state,
                                                   this, std::placeholders::_1, std::placeholders::_2));

        this->server.post("/save_state", std::bind(&visualweb_interface::service_save_state,
                                                   this, std::placeholders::_1, std::placeholders::_2));

        this->server.post("/dirlist", std::bind(&visualweb_interface::service_get_dirlist,
                                                this, std::placeholders::_1, std::placeholders::_2));

        this->server.post("/add_trajectory", std::bind(&visualweb_interface::service_add_trajectory,
                                                       this, std::placeholders::_1, std::placeholders::_2));

        this->server.post("/finish_trajectory", std::bind(&visualweb_interface::service_finish_trajectory,
                                                          this, std::placeholders::_1, std::placeholders::_2));

        this->server.post("/finish", std::bind(&visualweb_interface::service_finish,
                                               this, std::placeholders::_1, std::placeholders::_2));

        this->server.post("/try_global_relocate", std::bind(&visualweb_interface::service_try_global_relocate,
                                                            this, std::placeholders::_1, std::placeholders::_2));

        this->server.post("/optimize", std::bind(&visualweb_interface::service_optimize,
                                                 this, std::placeholders::_1, std::placeholders::_2));

    }
};

#endif
