#ifndef CYBER_WEBSERVER_HPP
#define CYBER_WEBSERVER_HPP

#include <string>
#include <queue>
#include <unordered_map>
#include <vector>
#include <functional>
#include <iostream>
#include <thread>
#include <sstream>
#include <regex>
#include <mutex>
#include <fstream>

#include "epset_manager.hpp"

#include "openssl/sha.h"
#include "openssl/evp.h"
#include "openssl/buffer.h"

#define HEADER_BUFFER_SIZE 4096

namespace cyber
{
    using HTTPHeaderMap = std::unordered_map<std::string, std::string>;

    enum class HttpMethod
    {
        GET = 0,
        POST,
        HEAD,
        OPTION,
        PUT,
        PATCH,
        DELETE,
        TRACE,
        CONNECT,
        Unknown,
    };

    enum class HttpCode
    {
        OK = 200,
        SwitchProtocol = 101,
        Foridden = 403,
        NotFound = 404,
        ServerError = 500,
    };

    class websocket
    {
        friend class webserver;

    private:
        std::shared_ptr<transport_tcp> sock;
        std::function<void()> callback_onopen;
        std::function<void(std::string &&)> callback_onmessage;
        std::function<void()> callback_onclose;
        std::mutex msg_sending_lock;
        std::queue<std::string> msg_sending;
        bool binary;

        void config(const std::shared_ptr<transport_tcp> &s)
        {
            if (this->sock != nullptr)
            {
                this->sock->close();
            }
            s->set_read(std::bind(&websocket::sock_read, this, std::placeholders::_1));
            s->set_write(std::bind(&websocket::sock_write, this, std::placeholders::_1));
            s->set_disconnect(std::bind(&websocket::sock_disconnect, this, std::placeholders::_1));
            s->request_read();
            this->sock = s;
            if (this->callback_onopen != nullptr)
            {
                this->callback_onopen();
            }
        }

        void sock_read(const std::shared_ptr<transport_tcp> &c)
        {
            unsigned char header_buf[24];
            int pos = 0;
            auto len = c->read(header_buf, 2);
            if (len == -1)
            {
                return;
            }
            // bool fin = header_buf[pos] & 0b10000000;
            unsigned char opc = header_buf[pos] & 0b00001111;
            pos += 1;
            // opc=0x0: more
            // opc=0x1: text
            // opc=0x2: binary
            // opc=0x3-7: reserved
            // opc=0x8: close
            // opc=0x9: ping
            // opc=0xA: pong

            if (opc == 0x01 || opc == 0x02)
            {
                unsigned char mask_key[4];
                bool masked = header_buf[pos] & 0b10000000;
                unsigned char payload_len = header_buf[pos] & 0b01111111;
                pos += 1;
                unsigned long long total_len = 0;
                unsigned char *p_total_len = (unsigned char *)&total_len;
                if (payload_len == 126)
                {
                    auto len = c->read(header_buf + pos, 6);
                    if (len == -1)
                    {
                        return;
                    }
                    p_total_len[0] = header_buf[pos + 1];
                    p_total_len[1] = header_buf[pos + 0];
                    pos += 2;
                }
                else if (payload_len == 127)
                {
                    auto len = c->read(header_buf + pos, 12);
                    if (len == -1)
                    {
                        return;
                    }
                    p_total_len[0] = header_buf[pos + 7];
                    p_total_len[1] = header_buf[pos + 6];
                    p_total_len[2] = header_buf[pos + 5];
                    p_total_len[3] = header_buf[pos + 4];
                    p_total_len[4] = header_buf[pos + 3];
                    p_total_len[5] = header_buf[pos + 2];
                    p_total_len[6] = header_buf[pos + 1];
                    p_total_len[7] = header_buf[pos + 0];
                    pos += 8;
                }
                else
                {
                    auto len = c->read(header_buf + pos, 4);
                    if (len == -1)
                    {
                        return;
                    }
                    total_len = payload_len;
                }

                if (masked)
                {
                    mask_key[0] = header_buf[pos];
                    mask_key[1] = header_buf[pos + 1];
                    mask_key[2] = header_buf[pos + 2];
                    mask_key[3] = header_buf[pos + 3];
                    pos += 4;
                }

                std::string data;
                if (total_len > 0)
                {
                    data.resize(total_len);
                    unsigned long long hasGot = 0;
                    while (total_len - hasGot > 0)
                    {
                        auto len = c->read(&data.at(0) + hasGot, total_len - hasGot);
                        if (len == -1)
                        {
                            return;
                        }
                        hasGot += len;
                    }

                    for (size_t i = 0; i < data.size(); i++)
                    {
                        data.at(i) = data.at(i) ^ mask_key[i % 4];
                    }
                }

                if (this->callback_onmessage != nullptr)
                {
                    this->callback_onmessage(std::move(data));
                }
            }
        }

        void sock_disconnect(const std::shared_ptr<transport_tcp> &c)
        {
            this->sock = nullptr;
            if (this->callback_onclose != nullptr)
            {
                this->callback_onclose();
            }
        }

        void sock_write(const std::shared_ptr<transport_tcp> &c)
        {
            this->msg_sending_lock.lock();
            if (this->msg_sending.empty())
            {
                this->msg_sending_lock.unlock();
                return;
            }

            std::string msg = std::move(this->msg_sending.front());
            this->msg_sending.pop();

            this->msg_sending_lock.unlock();

            unsigned char pos = 0;
            unsigned char header[32];

            header[pos] = 0x80;
            if (this->binary)
                header[pos] |= 0x02;
            else
                header[pos] |= 0x01;

            pos += 1;
            if (msg.size() <= 125)
            {
                header[pos] = msg.size();
                pos += 1;
            }
            else if (msg.size() > 125 && msg.size() <= 65535)
            {
                header[pos] = 126;
                pos += 1;
                unsigned short t = msg.size();
                header[pos + 0] = *((unsigned char *)&t + 1);
                header[pos + 1] = *((unsigned char *)&t + 0);
                pos += 2;
            }
            else
            {
                header[1] = 127;
                pos += 1;
                unsigned long long t = msg.size();
                header[pos + 0] = *((unsigned char *)&t + 7);
                header[pos + 1] = *((unsigned char *)&t + 6);
                header[pos + 2] = *((unsigned char *)&t + 5);
                header[pos + 3] = *((unsigned char *)&t + 4);
                header[pos + 4] = *((unsigned char *)&t + 3);
                header[pos + 5] = *((unsigned char *)&t + 2);
                header[pos + 6] = *((unsigned char *)&t + 1);
                header[pos + 7] = *((unsigned char *)&t + 0);
                pos += 8;
            }

            unsigned long long hasSent = 0;
            while (pos - hasSent > 0)
            {
                auto len = c->write(header + hasSent, pos - hasSent);
                if (len == -1)
                {
                    return;
                }
                hasSent += len;
            }

            hasSent = 0;
            while (msg.size() - hasSent > 0)
            {
                auto len = c->write(&msg.at(0) + hasSent, msg.size() - hasSent);
                if (len == -1)
                {
                    return;
                }
                hasSent += len;
            }

            this->msg_sending_lock.lock();
            if (this->msg_sending.empty())
            {
                this->sock->request_read();
            }
            else
            {
                this->sock->request_write();
            }
            this->msg_sending_lock.unlock();
        }

    public:
        websocket()
        {
        }

        ~websocket()
        {
        }

        void set_binary(bool bin)
        {
            this->binary = bin;
        }

        void set_onopen(std::function<void(void)> onopen)
        {
            this->callback_onopen = onopen;
        }

        void set_onmessage(std::function<void(std::string &&)> onmsg)
        {
            this->callback_onmessage = onmsg;
        }

        void set_onclose(std::function<void(void)> onclose)
        {
            this->callback_onclose = onclose;
        }

        void write(std::string &&data)
        {
            if (this->sock != nullptr)
            {
                this->msg_sending_lock.lock();
                this->msg_sending.emplace(std::move(data));
                this->msg_sending_lock.unlock();
                this->sock->request_write();
            }
        }

        void write(std::string &data)
        {
            if (this->sock != nullptr)
            {
                this->msg_sending_lock.lock();
                this->msg_sending.emplace(data);
                this->msg_sending_lock.unlock();
                this->sock->request_write();
            }
        }
    };

    class http_request
    {
    public:
        HttpMethod method;
        HTTPHeaderMap headers;
        std::string content;
        std::string url;
        std::shared_ptr<transport_tcp> sock;
        http_request() = default;
        ~http_request() = default;
    };

    class http_response
    {
    public:
        HttpCode code;
        HTTPHeaderMap headers;

        std::string header;
        std::string content;
        std::string url;

        bool ws_upgrade = false;
        std::shared_ptr<websocket> ws;
        bool hasSentHeader = false;
        std::size_t hasSent = 0;
        http_response()
        {
            code = HttpCode::OK;
            this->hasSentHeader = false;
            this->hasSent = 0;
        };
        ~http_response() = default;

        void add_header(std::string &&key, std::string &&val)
        {
            this->headers[key] = val;
        }

        void del_header(std::string &&key)
        {
            this->headers.erase(key);
        }

        void detect_content_length()
        {
            this->add_header("Content-Length", std::to_string(this->content.size()));
        }

        // Return: true=binnary false=text
        bool detect_content_type(std::string &suffix)
        {
            std::string content_type;
            bool ret = true;
            if (suffix == ".html")
            {
                content_type = "text/html";
                ret = false;
            }
            else if (suffix == ".htm")
            {
                content_type = "text/html";
                ret = false;
            }
            else if (suffix == ".js")
            {
                content_type = "text/javascript";
                ret = false;
            }
            else if (suffix == ".css")
            {
                content_type = "text/css";
                ret = false;
            }
            else if (suffix == ".avi")
            {
                content_type = "video/x-msvideo";
                ret = true;
            }
            else if (suffix == ".bmp")
            {
                content_type = "image/bmp";
                ret = true;
            }
            else if (suffix == ".csv")
            {
                content_type = "text/csv";
                ret = false;
            }
            else if (suffix == ".gif")
            {
                content_type = "image/gif";
                ret = true;
            }
            else if (suffix == ".ico")
            {
                content_type = "image/vnd.microsoft.icon";
                ret = true;
            }
            else if (suffix == ".jpg")
            {
                content_type = "image/jpeg";
                ret = true;
            }
            else if (suffix == ".jpeg")
            {
                content_type = "image/jpeg";
                ret = true;
            }
            else if (suffix == ".json")
            {
                content_type = "application/json";
                ret = false;
            }
            else if (suffix == ".mp3")
            {
                content_type = "audio/mpeg";
                ret = true;
            }
            else if (suffix == ".mp4")
            {
                content_type = "video/mp4";
                ret = true;
            }
            else if (suffix == ".png")
            {
                content_type = "image/png";
                ret = true;
            }
            else if (suffix == ".svg")
            {
                content_type = "image/svg+xml";
                ret = true;
            }
            else if (suffix == ".tif")
            {
                content_type = "image/tiff";
                ret = true;
            }
            else if (suffix == ".tiff")
            {
                content_type = "image/tiff";
                ret = true;
            }
            else if (suffix == ".ts")
            {
                content_type = "video/mp2t";
                ret = true;
            }
            else if (suffix == ".wav")
            {
                content_type = "audio/wav";
                ret = true;
            }

            else if (suffix == ".webp")
            {
                content_type = "image/webp";
                ret = true;
            }
            else if (suffix == ".woff")
            {
                content_type = "font/woff";
                ret = true;
            }
            else if (suffix == ".woff2")
            {
                content_type = "font/woff2";
                ret = true;
            }
            else if (suffix == ".zip")
            {
                content_type = "application/zip";
                ret = true;
            }
            else
            {
                content_type = "application/octet-stream";
                ret = true;
            }
            this->add_header("Content-Type", std::move(content_type));
            return ret;
        }

        void generate()
        {
            this->header.reserve(4096);
            auto code_str = std::to_string((int)this->code);
            this->header.append(std::string("HTTP/1.1 ") + code_str + " OK\r\n");
            this->add_header("Server", "Cyber Web Server");
            for (auto i = this->headers.begin(); i != this->headers.end(); i++)
            {
                this->header += (*i).first + ":" + (*i).second + "\r\n";
            }
            this->header += "\r\n";
        }
    };

    using HttpRequestPtr = std::shared_ptr<http_request>;
    using HttpResponsePtr = std::shared_ptr<http_response>;
    using HttpHandlerFn = std::function<void(const HttpRequestPtr &, const HttpResponsePtr &)>;

    class webserver
    {
    private:
        epset_manager epmanager;
        std::string root;
        std::shared_ptr<transport_tcp> server;

        // Request Handle
        std::mutex request_queue_lock;
        std::queue<std::shared_ptr<http_request>> request_queue;
        bool th_request_enable;
        std::vector<std::thread> th_request_pool;
        // std::thread th_request;

        // Response Handle
        std::mutex response_map_lock;
        std::unordered_map<int, std::shared_ptr<http_response>> response_map;

        // Handler
        std::unordered_map<std::string, HttpHandlerFn> handlers_get;
        std::unordered_map<std::string, HttpHandlerFn> handlers_post;
        std::unordered_map<std::string, std::shared_ptr<websocket>> websockets;

        std::string base64_encode(const unsigned char *bytes_to_encode, unsigned int in_len)
        {
            BIO *b64, *mem;
            BUF_MEM *bptr;
            b64 = BIO_new(BIO_f_base64());
            mem = BIO_new(BIO_s_mem());
            BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
            BIO_push(b64, mem);
            BIO_write(b64, bytes_to_encode, in_len);
            BIO_flush(b64);
            BIO_get_mem_ptr(mem, &bptr);
            std::string ret(bptr->data, bptr->length);
            BIO_free_all(b64);
            return ret;
        }

        std::string calculate_websocket_accept(const std::string &sec_websocket_key)
        {
            unsigned char sha1_digest[SHA_DIGEST_LENGTH];
            std::string combined = sec_websocket_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
            SHA1((unsigned char *)&combined.at(0), combined.length(), sha1_digest);
            return base64_encode(sha1_digest, SHA_DIGEST_LENGTH);
        }

        std::vector<std::string> split_string_by_find(const std::string &str, char delimiter)
        {
            std::vector<std::string> tokens;
            size_t start = 0;
            size_t end = 0;
            while ((end = str.find(delimiter, start)) != std::string::npos)
            {
                tokens.push_back(str.substr(start, end - start));
                start = end + 1;
            }
            if (start < str.length())
            {
                tokens.push_back(str.substr(start));
            }
            return tokens;
        }

        bool parse_header_protocol(std::string &line, std::shared_ptr<http_request> &req)
        {
            auto result = this->split_string_by_find(line, ' ');
            if (result.size() == 3)
            {
                // Method
                if (result.at(0) == "GET")
                {
                    req->method = HttpMethod::GET;
                }
                else if (result.at(0) == "POST")
                {
                    req->method = HttpMethod::POST;
                }
                else if (result.at(0) == "HEAD")
                {
                    req->method = HttpMethod::HEAD;
                }
                else if (result.at(0) == "OPTION")
                {
                    req->method = HttpMethod::OPTION;
                }
                else if (result.at(0) == "PUT")
                {
                    req->method = HttpMethod::PUT;
                }
                else if (result.at(0) == "PATCH")
                {
                    req->method = HttpMethod::PATCH;
                }
                else if (result.at(0) == "DELETE")
                {
                    req->method = HttpMethod::DELETE;
                }
                else if (result.at(0) == "TRACE")
                {
                    req->method = HttpMethod::TRACE;
                }
                else if (result.at(0) == "CONNECT")
                {
                    req->method = HttpMethod::CONNECT;
                }
                else
                {
                    req->method = HttpMethod::Unknown;
                }
                // Url
                req->url = result.at(1);
                return true;
            }
            return false;
        }

        void parse_header_line(const std::string &line, HTTPHeaderMap &headers)
        {
            size_t colonPos = line.find(':');
            if (colonPos != std::string::npos)
            {
                std::string key = line.substr(0, colonPos);
                std::string value = line.substr(colonPos + 1);
                key = std::regex_replace(key, std::regex("\\s+"), "");
                value = std::regex_replace(value, std::regex("\\s+"), ""); // space for value is also not allowed.
                headers[key] = value;
            }
        }

        void event_accept(const std::shared_ptr<transport_tcp> &c)
        {
            c->set_disconnect(std::bind(&webserver::event_disconnect, this, std::placeholders::_1));
            c->set_read(std::bind(&webserver::event_read, this, std::placeholders::_1));
            c->set_write(std::bind(&webserver::event_write, this, std::placeholders::_1));
            c->request_read();
        }

        void event_disconnect(const std::shared_ptr<transport_tcp> &c)
        {
        }

        void event_read(const std::shared_ptr<transport_tcp> &c)
        {
            char header_buf[HEADER_BUFFER_SIZE];
            std::size_t hasGot = 0;
            std::string left_content;
            while (true)
            {
                if (hasGot >= HEADER_BUFFER_SIZE)
                {
                    hasGot = 0;
                }
                auto len = c->read(header_buf + hasGot, HEADER_BUFFER_SIZE - hasGot);
                if (len == -1)
                {
                    return;
                }
                hasGot += len;
                auto endofheader = strstr(header_buf, "\r\n\r\n");
                if (endofheader != nullptr)
                {
                    *endofheader = '\0';
                    left_content = std::string(endofheader + 4, hasGot - (endofheader - header_buf) - 4);
                    break;
                }
            }

            std::shared_ptr<http_request> req = std::make_shared<http_request>();
            req->sock = c;
            auto header = std::string(header_buf, strlen(header_buf));

            std::istringstream iss(header);
            std::string line;
            // process header protocol
            std::getline(iss, line);
            if (!this->parse_header_protocol(line, req))
            {
                return;
            }

            // process headers
            while (std::getline(iss, line) && !line.empty())
            {
                parse_header_line(line, req->headers);
            }
            // recv content
            auto _content_len = req->headers["Content-Length"];
            if (_content_len.empty())
            {
                _content_len = req->headers["content-length"];
            }
            if (!_content_len.empty())
            {
                std::size_t content_len = 0;
                std::stringstream ss;
                ss << _content_len;
                ss >> content_len;
                req->content.resize(content_len);
                std::copy(left_content.cbegin(), left_content.cend(), req->content.begin());
                std::size_t hasGot = left_content.size();
                while (hasGot < content_len)
                {
                    auto len = c->read(&req->content.at(0) + hasGot, content_len - hasGot);
                    if (len == -1)
                    {
                        return;
                    }
                    hasGot += len;
                }
            }

            // push to queue
            request_queue_lock.lock();
            this->request_queue.push(req);
            request_queue_lock.unlock();
            c->request_read();
        }

        void event_write(const std::shared_ptr<transport_tcp> &c)
        {
            bool send_over = false;
            std::shared_ptr<http_response> res;
            this->response_map_lock.lock();
            if (this->response_map.find(c->sock) != this->response_map.end())
            {
                res = this->response_map[c->sock];
            }
            this->response_map_lock.unlock();

            if (res == nullptr)
            {
                c->request_read();
                return;
            }

            if (res->ws_upgrade)
            {
                if (!res->hasSentHeader)
                {
                    if (res->header.size() == 0)
                    {
                        res->hasSent = 0;
                        res->generate();
                    }
                    auto wlen = c->write(&res->header.at(0) + res->hasSent, res->header.size() - res->hasSent);
                    if (wlen == -1)
                    {
                        return;
                    }
                    res->hasSent += wlen;
                    if (res->hasSent >= res->header.size())
                    {
                        res->hasSentHeader = true;
                        res->ws->config(c);
                        res->hasSent = 0;
                        send_over = true;
                    }
                }
            }
            else
            {
                if (!res->hasSentHeader)
                {
                    // send header
                    if (res->header.size() == 0)
                    {
                        res->generate();
                        res->hasSent = 0;
                    }
                    auto wlen = c->write(&res->header.at(0) + res->hasSent, res->header.size() - res->hasSent);
                    if (wlen == -1)
                    {
                        return;
                    }
                    res->hasSent += wlen;
                    if (res->hasSent >= res->header.size())
                    {
                        res->hasSentHeader = true;
                        res->hasSent = 0;
                        if (res->content.empty())
                        {
                            send_over = true;
                        }
                    }
                }
                else
                {
                    // send content
                    auto wlen = c->write(&res->content.at(0) + res->hasSent, res->content.size() - res->hasSent);
                    if (wlen == -1)
                    {
                        return;
                    }
                    res->hasSent += wlen;
                    if (res->hasSent >= res->content.size())
                    {
                        send_over = true;
                    }
                }
            }
            if (send_over)
            {
                this->response_map_lock.lock();
                this->response_map.erase(c->sock);
                this->response_map_lock.unlock();
                c->request_read();
            }
            else
            {
                c->request_write();
            }
        }

        bool handle_websocket(std::shared_ptr<http_request> &req, std::shared_ptr<http_response> &res)
        {
            if (this->websockets.find(req->url) == this->websockets.end())
            {
                return false;
            }
            std::string ws_key;
            if (req->headers.at("Upgrade") == "websocket")
            {
                ws_key = req->headers["Sec-WebSocket-Key"];
            }
            res->ws = this->websockets.at(req->url);
            res->ws_upgrade = true;
            res->code = HttpCode::SwitchProtocol;
            res->add_header("Upgrade", "websocket");
            res->add_header("Connection", "Upgrade");
            res->add_header("Sec-WebSocket-Accept", calculate_websocket_accept(ws_key));
            return true;
        }

        bool handle_route(std::shared_ptr<http_request> &req, std::shared_ptr<http_response> &res)
        {
            if (req->method == HttpMethod::GET)
            {
                if (this->handlers_get.find(req->url) == this->handlers_get.end())
                {
                    return false;
                }
                auto &fn = this->handlers_get.at(req->url);
                fn(req, res);
            }
            else if (req->method == HttpMethod::POST)
            {
                if (this->handlers_post.find(req->url) == this->handlers_post.end())
                {
                    return false;
                }
                auto &fn = this->handlers_post.at(req->url);
                fn(req, res);
            }
            return true;
        }

        bool handle_file(std::shared_ptr<http_request> &req, std::shared_ptr<http_response> &res)
        {
            std::ifstream in;

            if (req->url == "/")
            {
                req->url = "/index.html";
            }

            // detect file type
            auto surffix_pos = req->url.find_last_of('.');
            bool isBinary = true;
            if (surffix_pos != std::string::npos)
            {
                auto suffix = req->url.substr(surffix_pos);
                isBinary = res->detect_content_type(suffix);
            }

            if (isBinary)
            {
                in.open(this->root + req->url, std::ios::binary | std::ios::in);
            }
            else
            {
                in.open(this->root + req->url, std::ios::in);
            }

            if (in.is_open())
            {
                res->code = HttpCode::OK;
                in.seekg(0, std::ios::end);
                auto file_len = in.tellg();
                in.seekg(0, std::ios::beg);
                res->content.resize(file_len);
                in.read(&res->content.at(0), file_len);
                in.close();
                return true;
            }
            else
            {
                res->code = HttpCode::NotFound;
                return false;
            }
        }

        void th_request_handler()
        {
            while (this->th_request_enable)
            {
                std::shared_ptr<http_request> req;
                this->request_queue_lock.lock();
                if (!this->request_queue.empty())
                {
                    req = this->request_queue.front();
                    this->request_queue.pop();
                }
                this->request_queue_lock.unlock();

                if (req == nullptr)
                {
                    continue;
                }

                std::shared_ptr<http_response> res = std::make_shared<http_response>();

                bool processed = false;

                if (!processed)
                {
                    processed = this->handle_websocket(req, res);
                }

                if (!processed)
                {
                    processed = this->handle_route(req, res);
                }

                if (!processed)
                {
                    processed = this->handle_file(req, res);
                }

                res->detect_content_length();
                response_map_lock.lock();
                this->response_map[req->sock->sock] = res;
                response_map_lock.unlock();
                req->sock->request_write();
            }
        }

    public:
        webserver()
        {
            this->server = epmanager.create_transport_tcp();
        }

        ~webserver()
        {
            this->th_request_enable = false;
            for (auto i = this->th_request_pool.begin(); i != this->th_request_pool.end(); i++)
            {
                (*i).join();
            }
            this->server->close();
        }

        void set_root(std::string &&rootpath)
        {
            this->root = std::move(rootpath);
        }

        bool start(unsigned short port, int thread_num = 1, int request_thread_num = 1)
        {
            auto ret = server->listen(port, 1000, std::bind(&webserver::event_accept, this, std::placeholders::_1));
            if (ret)
            {
                epmanager.start(thread_num);
                this->th_request_enable = true;
                for (int i = 0; i < request_thread_num; i++)
                {
                    this->th_request_pool.emplace_back(std::thread(&webserver::th_request_handler, this));
                }
                CYBER_INFO("Web Server Started!");
            }
            return ret;
        }

        void get(std::string &&url, HttpHandlerFn fn)
        {
            this->handlers_get[url] = fn;
        }

        void post(std::string &&url, HttpHandlerFn fn)
        {
            this->handlers_post[url] = fn;
        }

        std::shared_ptr<websocket> web_socket(std::string &&url)
        {
            std::shared_ptr<websocket> ws = std::make_shared<websocket>();
            this->websockets[url] = ws;
            return ws;
        }
    };
}

#endif