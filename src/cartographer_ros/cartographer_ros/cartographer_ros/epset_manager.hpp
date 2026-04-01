#ifndef CYBER_SOCKET_TCP_HPP
#define CYBER_SOCKET_TCP_HPP

#include <memory>
#include <thread>
#include <vector>
#include <unordered_map>
#include <queue>
#include <functional>
#include <mutex>

#include "transport_tcp.hpp"

namespace cyber
{
    class epset_manager
    {
    private:
        epoll_set epset;
        std::mutex tcps_mutex;
        std::unordered_map<int, std::shared_ptr<transport_tcp>> tcps;
        std::thread epset_thread;
        bool thread_enable;

        std::mutex events_lock;
        std::queue<std::pair<int, int>> events;
        std::vector<std::thread> thead_pool;

        void add_to_epset(std::shared_ptr<transport_tcp> client)
        {
            tcps_mutex.lock();
            this->epset.add_socket(client->sock);
            this->tcps[client->sock] = client;
            tcps_mutex.unlock();
        }

        void del_from_epset(int fd)
        {
            tcps_mutex.lock();
            if (this->tcps.find(fd) != this->tcps.end())
            {
                this->tcps.erase(fd);
                this->epset.del_socket(fd);
            }
            tcps_mutex.unlock();
        }

        void epset_update()
        {
            while (this->thread_enable)
            {
                auto ev = this->epset.wait(50);
                events_lock.lock();
                for (auto i = ev.cbegin(); i != ev.cend(); i++)
                {
                    this->events.emplace((*i));
                }
                events_lock.unlock();
            }
        }

        void handle_event()
        {
            while (this->thread_enable)
            {
                std::pair<int, int> ev = std::make_pair<int, int>(-1, -1);
                events_lock.lock();
                if (!events.empty())
                {
                    ev = std::move(events.front());
                    events.pop();
                }
                events_lock.unlock();

                if (ev.first == -1)
                {
                    continue;
                }

                std::shared_ptr<transport_tcp> tcp;
                tcps_mutex.lock();
                if (this->tcps.find(ev.first) == this->tcps.end())
                {
                    continue;
                }
                tcp = this->tcps[ev.first];
                tcps_mutex.unlock();

                if (tcp == nullptr)
                {
                    continue;
                }
                tcp->handle_epoll_events(ev.second);
            }
        }

    public:
        epset_manager()
        {
            this->thread_enable = false;
        };
        ~epset_manager()
        {
            if (this->thread_enable)
            {
                this->thread_enable = false;
                this->epset_thread.join();
                for (auto i = this->thead_pool.begin(); i != this->thead_pool.end(); i++)
                {
                    (*i).join();
                }
            }
        }

        void start(int thread_num = 1)
        {
            this->thread_enable = true;
            this->epset_thread = std::thread(&epset_manager::epset_update, this);
            for (int i = 0; i < thread_num; i++)
            {
                this->thead_pool.emplace_back(std::thread(&epset_manager::handle_event, this));
            }
        }

        std::shared_ptr<transport_tcp> create_transport_tcp()
        {
            std::shared_ptr<transport_tcp> t = std::make_shared<transport_tcp>(&this->epset,
                                                                               std::bind(&epset_manager::add_to_epset, this, std::placeholders::_1),
                                                                               std::bind(&epset_manager::del_from_epset, this, std::placeholders::_1));
            return t;
        }
    };
} // namespace cyber

#endif