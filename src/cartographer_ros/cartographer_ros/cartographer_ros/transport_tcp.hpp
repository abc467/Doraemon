#ifndef CYBER_TRANSPORT_TCP_HPP
#define CYBER_TRANSPORT_TCP_HPP

#include <functional>

#include "io.hpp"
#include "epoll_set.hpp"
#include "logger.hpp"

namespace cyber
{
    class transport_tcp : public std::enable_shared_from_this<transport_tcp>
    {
        friend class epset_manager;
        friend class transport_tcp_guard_read;
        friend class transport_tcp_guard_write;
        using callback_t = std::function<void(std::shared_ptr<transport_tcp>)>;
        using add_to_epset_cb_t = std::function<void(std::shared_ptr<transport_tcp>)>;
        using del_from_epset_cb_t = std::function<void(int)>;

    public:
        int sock;
        bool connected;
        unsigned short connected_port;
        unsigned short server_port;

    private:
        epoll_set *epset = 0;
        add_to_epset_cb_t add_to_epset_cb;
        del_from_epset_cb_t del_from_epset_cb;

        callback_t accept_cb;
        callback_t disconnect_cb;
        callback_t read_cb;
        callback_t write_cb;

        std::mutex close_lock;
        volatile bool closed = true;
        volatile bool is_server = false;

    private:
        void setup_socket(int fd)
        {
            this->closed = false;
            this->sock = fd;
            io::socket_set_non_block(this->sock);
            io::socket_set_send_bufsize(this->sock, 64 * 1024);
            io::socket_set_recv_bufsize(this->sock, 64 * 1024);
            io::socket_set_keepalive(this->sock, true, 30, 5, 3);
            // io::socket_set_nodelay(this->sock, true);
            if (this->epset)
            {
                if (this->add_to_epset_cb)
                {
                    add_to_epset_cb(this->shared_from_this());
                }
            }
        }

        std::shared_ptr<transport_tcp> accept()
        {
            sockaddr_in addr;
            socklen_t addrlen = sizeof(addr);
            int client = ::accept(this->sock, (sockaddr *)&addr, &addrlen);
            if (client < 0)
            {
                CYBER_ERROR("accept error: %s", strerror(errno));
            }
            if (io::socket_set_non_block(client) == -1)
            {
                CYBER_ERROR("set non-block error %s", strerror(errno));
            }
            std::shared_ptr<transport_tcp> t = std::make_shared<transport_tcp>(this->epset,
                                                                               add_to_epset_cb,
                                                                               del_from_epset_cb);
            t->setup_socket(client);
            return t;
        }

        void handle_epoll_events(int event)
        {
            if (event & EPOLLIN)
            {
                if (this->is_server)
                {
                    this->accept_cb(this->accept());
                    this->request_read();
                }
                else
                {
                    if (this->read_cb)
                    {
                        this->read_cb(this->shared_from_this());
                    }
                }
            }
            else if (event & EPOLLOUT)
            {
                if (this->write_cb)
                {
                    this->write_cb(this->shared_from_this());
                }
            }
            else if ((event & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)))
            {
                // EPOLLRDHUP: Linux kernel should be over 2.6.17
                this->close();
            }
        }

    public:
        transport_tcp(epoll_set *epset, add_to_epset_cb_t add_fn, del_from_epset_cb_t del_fn)
        {
            signal(SIGPIPE, SIG_IGN);
            this->sock = -1;
            this->epset = epset;
            this->add_to_epset_cb = add_fn;
            this->del_from_epset_cb = del_fn;
        };

        ~transport_tcp()
        {
            if (this->sock != -1)
            {
                this->close();
            }
        };

        void set_disconnect(const callback_t &cb) { disconnect_cb = cb; }
        void set_read(const callback_t &cb) { read_cb = cb; }
        void set_write(const callback_t &cb) { write_cb = cb; }

        void close()
        {
            if (this->close_lock.try_lock())
            {
                if (!this->closed)
                {
                    this->closed = true;
                    if (this->epset)
                    {
                        if (this->del_from_epset_cb)
                        {
                            del_from_epset_cb(this->sock);
                        }
                    }
                    ::shutdown(this->sock, SHUT_WR);
                    io::socket_close(this->sock);
                    if (this->disconnect_cb)
                    {
                        this->disconnect_cb(this->shared_from_this());
                    }
                    this->sock = -1;
                    this->epset = 0;
                }
                this->close_lock.unlock();
            }
        }

        int read(void *buf, size_t size)
        {
            int len = ::recv(this->sock, reinterpret_cast<char *>(buf), size, 0);
            if (len < 0)
            {
                if (errno == EAGAIN)
                {
                    len = 0;
                }
                else if (errno == ENOTCONN || errno == ECONNRESET)
                {
                    this->close();
                }
                else
                {
                    this->close();
                    CYBER_ERROR("tcp read error: %s", strerror(errno));
                }
            }
            else if (len == 0)
            {
                this->close();
                len = -1;
            }
            return len;
        }

        int write(const void *buf, size_t size)
        {
            int len = ::send(this->sock, reinterpret_cast<const char *>(buf), size, 0);
            if (len < 0)
            {
                if (errno == EAGAIN)
                {
                    len = 0;
                }
                else if (errno == ENOTCONN || errno == ECONNRESET)
                {
                    this->close();
                }
                else
                {
                    this->close();
                    CYBER_ERROR("tcp write error: %s", strerror(errno));
                }
            }
            else if (len == 0)
            {
                len = -1;
                this->close();
            }
            return len;
        }

        // read and write cannot set at the same time.
        void request_read()
        {
            if (this->epset != 0)
            {
                this->epset->set_event(this->sock, EPOLLIN, true);
            }
        }

        // read and write cannot set at the same time.
        void request_write()
        {
            if (this->epset != 0)
            {
                this->epset->set_event(this->sock, EPOLLOUT, true);
            }
        }

        bool connect(const char *ip, unsigned short port)
        {
            this->is_server = false;
            this->connected = false;
            this->sock = socket(AF_INET, SOCK_STREAM, 0);
            if (this->sock < 0)
            {
                CYBER_ERROR("socket() error %s", strerror(errno));
                return false;
            }
            io::socket_set_reuse(this->sock);
            sockaddr_in addr;
            socklen_t addrlen = sizeof(addr);
            bzero(&addr, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = inet_addr(ip);
            addr.sin_port = htons(port);
            int ret = ::connect(this->sock, (sockaddr *)&addr, addrlen);
            if (ret == -1)
            {
                ::close(this->sock);
                CYBER_ERROR("tcp connect to server failed %s", strerror(errno));
                return false;
            }
            getsockname(this->sock, (sockaddr *)&addr, &addrlen);
            this->connected_port = ntohs(addr.sin_port);
            this->setup_socket(this->sock);
            this->connected = true;
            return true;
        }

        bool listen(unsigned short port, int backlog, callback_t accept_cb)
        {
            this->is_server = true;
            this->accept_cb = accept_cb;
            this->sock = socket(AF_INET, SOCK_STREAM, 0);
            if (this->sock < 0)
            {
                CYBER_ERROR("socket() error %s", strerror(errno));
                return false;
            }
            io::socket_set_reuse(this->sock);
            sockaddr_in addr;
            socklen_t addrlen = sizeof(addr);
            bzero(&addr, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_ANY);
            addr.sin_port = htons(port);
            if (::bind(this->sock, (sockaddr *)&addr, addrlen) == -1)
            {
                ::close(this->sock);
                CYBER_ERROR("tcp bind failed %s", strerror(errno));
                return false;
            }
            if (::listen(this->sock, backlog) == -1)
            {
                ::close(this->sock);
                CYBER_ERROR("tcp bind failed %s", strerror(errno));
                return false;
            }
            // get listened info
            getsockname(this->sock, (sockaddr *)&addr, &addrlen);
            this->server_port = ntohs(addr.sin_port);
            this->setup_socket(this->sock);
            this->request_read();
            return true;
        }
    };

    class transport_tcp_client
    {
    private:
        int sock;

    public:
        bool connected = false;
        short connected_port;

        transport_tcp_client() = default;

        ~transport_tcp_client() = default;

        void disconnect()
        {
            ::shutdown(this->sock, SHUT_WR);
            ::close(this->sock);
            this->connected = false;
        }

        bool connect(const char *ip, unsigned short port)
        {
            if (this->connected)
            {
                this->disconnect();
            }

            signal(SIGPIPE, SIG_IGN);
            this->sock = socket(AF_INET, SOCK_STREAM, 0);
            io::socket_set_reuse(this->sock);
            sockaddr_in addr;
            socklen_t addrlen = sizeof(addr);
            bzero(&addr, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = inet_addr(ip);
            addr.sin_port = htons(port);
            int ret = ::connect(this->sock, (sockaddr *)&addr, addrlen);
            if (ret == -1)
            {
                this->disconnect();
                return false;
            }
            getsockname(this->sock, (sockaddr *)&addr, &addrlen);
            this->connected_port = ntohs(addr.sin_port);
            this->connected = true;
            return true;
        }

        int write(const void *buf, size_t size)
        {
            int len = ::send(this->sock, reinterpret_cast<const char *>(buf), size, 0);
            if (len < 0)
            {
                this->disconnect();
                return -1;
            }
            return len;
        }

        int read(void *buf, size_t size)
        {
            int len = ::recv(this->sock, reinterpret_cast<char *>(buf), size, 0);
            if (len < 0)
            {
                this->disconnect();
                return -1;
            }
            return len;
        }
    };
}

#endif