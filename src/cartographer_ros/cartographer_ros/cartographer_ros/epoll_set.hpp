#ifndef CYBER_EPOLL_SET_HPP
#define CYBER_EPOLL_SET_HPP

#include <unordered_map>
#include "io.hpp"

namespace cyber
{
    class epoll_set
    {
    private:
        int epfd;

    public:
        epoll_set()
        {
            this->epfd = io::watcher_create();
        };
        ~epoll_set()
        {
            io::watcher_close(this->epfd);
        };

        void add_socket(int fd)
        {
            io::socket_add_to_watcher(this->epfd, fd);
        }

        void del_socket(int fd)
        {
            io::socket_del_from_watcher(this->epfd, fd);
        }

        void set_event(int fd, int ev, bool oneshot)
        {
            io::socket_set_events(this->epfd, fd, ev, oneshot);
        }

        std::unordered_map<int, int> wait(int timeout)
        {
            std::unordered_map<int, int> event;
            auto ofds = io::watcher_watch(this->epfd, timeout);
            for (auto i = ofds->begin(); i != ofds->end(); i++)
            {
                int fd = (*i).data.fd;
                int ev = (*i).events;
                event[fd] = ev;
            }
            return event;
        }
    };
}

#endif