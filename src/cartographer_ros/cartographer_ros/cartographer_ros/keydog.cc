#include "keydog.h"
#include <string>
#include <array>
#include <functional>
#include <regex>
#include <iostream>

#include "Vikey.h"

namespace keydog
{
    std::string exec(const char *cmd)
    {
        std::array<char, 128> buffer;
        std::string result;
        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
        if (!pipe)
        {
            throw std::runtime_error("popen() failed!");
        }
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr)
        {
            result += buffer.data();
        }
        return result;
    }

    std::string get_blkid()
    {
        auto str0 = exec("blkid");
        std::regex reg0(R"(UUID="\S{36}" TYPE="ext4" PARTUUID="\S{36}")", std::regex_constants::ECMAScript);
        std::regex reg1(R"(\S{8}-\S{4}-\S{4}-\S{4}-\S{12})", std::regex_constants::ECMAScript);
        std::string blkuuid;
        std::smatch r;
        std::regex_search(str0, r, reg0);
        if (r.size() > 0)
        {
            std::string str1 = r[0];
            std::regex_search(str1, r, reg1);
            if (r.size() > 0)
            {
                blkuuid = r[0];
            }
        }
        return blkuuid;
    }

    class defer
    {
    private:
        std::function<void()> cb;

    public:
        defer(std::function<void()> _cb) : cb(_cb) {}
        ~defer()
        {
            if (cb)
            {
                cb();
            }
        }
    };

    bool validate_key()
    {
        DWORD dog_count = 0;
        DWORD dog_exist = VikeyFind(&dog_count);
        if (dog_count != 1 || dog_exist != 0)
        {
            printf("Please insert Key Dog\n");
            return false;
        }
        const char *pwd = "12345678";
        DWORD ret_login = VikeyUserLogin(0, const_cast<char *>(pwd));
        if (ret_login != 0)
        {
            return false;
        }
        defer _defer([]()
                     { VikeyLogoff(0); });
        // check Hardware ID
        DWORD hid = 0;
        VikeyGetHID(0, &hid);
        if (hid != 300284316)
        {
            return false;
        }
        // check software id
        char sid[8];
        VikeyGetSoftIDString(0, sid);
        if (strcmp(sid, "SOFTSLAM") != 0)
        {
            return false;
        }
        // check deadline
        BYTE datetime_valid = 0;
        VikeyCheckValidTime(0, &datetime_valid);
        if (datetime_valid == 0)
        {
            return false;
        }
        // check device uuid
        std::string blkuuid = get_blkid();
        std::cout << "CurUUID=" << blkuuid << std::endl;
        std::string hw_blkuuid;
        hw_blkuuid.resize(36, 0);
        VikeyReadData(0, 10000, 36, (BYTE *)&hw_blkuuid.at(0));
        std::cout << "KeyUUID=" << hw_blkuuid << std::endl;
        if (blkuuid.compare(hw_blkuuid) != 0)
        {
            return false;
        }
        return true;
    }
} // namespace
