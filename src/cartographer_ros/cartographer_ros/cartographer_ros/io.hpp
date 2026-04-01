#ifndef CYBER_IO_HPP
#define CYBER_IO_HPP

#include <memory>
#include <vector>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/signal.h>
#include <sys/signalfd.h>
#include <fcntl.h>
#include <unistd.h>
#include <strings.h>

#include "logger.hpp"

namespace cyber
{
    class io
    {
    public:
        io() = default;
        ~io() = default;

        inline static int socket_set_reuse(int fd)
        {
            int opt_true = 1;
            if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt_true, sizeof(opt_true)) != 0)
            {
                CYBER_ERROR("setsockopt failed to set SO_REUSEADDR on socket: %s", strerror(errno));
                return -1;
            }

            if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt_true, sizeof(opt_true)) != 0)
            {
                CYBER_ERROR("setsockopt failed to set SO_REUSEPORT on socket: %s", strerror(errno));
                return -1;
            }
            return 0;
        }

        inline static int socket_set_recv_bufsize(int fd, int size)
        {
            if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size)) != 0)
            {
                CYBER_ERROR("setsockopt failed to set SO_RCVBUF on socket: %s", strerror(errno));
                return -1;
            }
            return 0;
        }

        inline static int socket_set_send_bufsize(int fd, int size)
        {
            if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) != 0)
            {
                CYBER_ERROR("setsockopt failed to set SO_SNDBUF on socket: %s", strerror(errno));
                return -1;
            }
            return 0;
        }

        inline static int socket_set_non_block(int fd)
        {
            return fcntl(fd, F_SETFL, O_NONBLOCK);
        }

        inline static int socket_set_nodelay(int fd, bool nodelay)
        {
            int flag = nodelay ? 1 : 0;
            return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
        }

        inline static int socket_set_keepalive(int fd, bool enbale, unsigned int idle, unsigned int interval, unsigned int count)
        {
            //idel: the max time for keep idle(no send and no recv);
            //interval: the interval time between send Heart Beat package;
            //count: if the count of failed to get response is over the threshold(count), disconnect!
            if (enbale)
            {
                int val = 1;
                if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<const char *>(&val), sizeof(val)) != 0)
                {
                    CYBER_ERROR("setsockopt failed to set SO_KEEPALIVE on socket: %s", strerror(errno));
                    return -1;
                }

                val = idle;
                if (setsockopt(fd, SOL_TCP, TCP_KEEPIDLE, &val, sizeof(val)) != 0)
                {
                    CYBER_ERROR("setsockopt failed to set TCP_KEEPIDLE on socket: %s", strerror(errno));
                    return -1;
                }

                val = interval;
                if (setsockopt(fd, SOL_TCP, TCP_KEEPINTVL, &val, sizeof(val)) != 0)
                {
                    CYBER_ERROR("setsockopt failed to set TCP_KEEPINTVL on socket: %s", strerror(errno));
                    return -1;
                }

                val = count;
                if (setsockopt(fd, SOL_TCP, TCP_KEEPCNT, &val, sizeof(val)) != 0)
                {
                    CYBER_ERROR("setsockopt failed to set TCP_KEEPCNT on socket: %s", strerror(errno));
                    return -1;
                }
            }
            else
            {
                int val = 0;
                if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<const char *>(&val), sizeof(val)) != 0)
                {
                    CYBER_ERROR("setsockopt failed to set SO_KEEPALIVE on socket: %s", strerror(errno));
                    return -1;
                }
            }
            return 0;
        }

        inline static int watcher_create()
        {
            int epfd = -1;
            epfd = ::epoll_create1(0);
            return epfd;
        }

        inline static void watcher_close(int epfd)
        {
            if (epfd > 0)
            {
                ::close(epfd);
            }
        }

        inline static std::shared_ptr<std::vector<epoll_event>>
        watcher_watch(int epfd, int timeout)
        {
            struct epoll_event ev[100];
            std::shared_ptr<std::vector<epoll_event>> ofds = std::make_shared<std::vector<epoll_event>>();

            int fd_cnt = ::epoll_wait(epfd, ev, 100, timeout);
            if (fd_cnt < 0)
            {
                if (fd_cnt != EINTR)
                {
                    CYBER_ERROR("Error in epoll_wait! %s", strerror(errno));
                }
            }
            else
            {
                ofds->reserve(fd_cnt);
                for (int i = 0; i < fd_cnt; i++)
                {
                    ofds->push_back(ev[i]);
                }
            }
            return ofds;
        }

        inline static int socket_add_to_watcher(int epfd, int fd)
        {
            struct epoll_event ev;
            bzero(&ev, sizeof(ev));
            ev.events = 0;
            ev.data.fd = fd;
            return ::epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
        }

        inline static int socket_del_from_watcher(int epfd, int fd)
        {
            return ::epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
        }

        inline static int socket_set_events(int epfd, int fd, int events, bool oneshot)
        {
            struct epoll_event ev;
            bzero(&ev, sizeof(ev));
            if (oneshot)
            {
                ev.events = events | EPOLLONESHOT;
            }
            else
            {
                ev.events = events;
            }
            ev.data.fd = fd;
            return ::epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
        }

        inline static void socket_close(int fd)
        {
            if (fd > 0)
            {
                close(fd);
            }
        }

        /**
         * Signal pair
         */

        inline static int signal_pair_create(int signal_pair[2])
        {
            signal_pair[0] = -1;
            signal_pair[1] = -1;

            if (pipe(signal_pair) != 0)
            {
                CYBER_ERROR("pipe() failed");
                return -1;
            }
            if (fcntl(signal_pair[0], F_SETFL, O_NONBLOCK) == -1)
            {
                CYBER_ERROR("fcntl() failed");
                return -1;
            }
            if (fcntl(signal_pair[1], F_SETFL, O_NONBLOCK) == -1)
            {
                CYBER_ERROR("fcntl() failed");
                return -1;
            }
            return 0;
        }

        inline static void signal_pair_close(int signal_pair[2])
        {
            ::close(signal_pair[0]);
            ::close(signal_pair[1]);
        }

        inline static ssize_t singnal_write(const int signal, const void *buf, const size_t size)
        {
            return write(signal, buf, size);
        }

        inline static ssize_t singnal_read(const int signal, void *buf, const size_t size)
        {
            return read(signal, buf, size);
        }
    };
}
#endif