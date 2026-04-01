#ifndef CYBER_TRANSPORT_HPP
#define CYBER_TRANSPORT_HPP

#include <functional>
#include <memory>

namespace cyber
{
    class transport
    {

    public:
        transport() = default;
        ~transport() = default;
        int sock = -1;
        virtual void handle_epoll_events(int event) = 0;
    };
}

#endif