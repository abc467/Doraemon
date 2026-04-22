#include "sml_config.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace
{
    std::string trim(const std::string &input)
    {
        const auto begin = input.find_first_not_of(" \t\r\n");
        if (begin == std::string::npos)
        {
            return "";
        }
        const auto end = input.find_last_not_of(" \t\r\n");
        return input.substr(begin, end - begin + 1);
    }

    std::string strip_comment(const std::string &line)
    {
        bool in_quotes = false;
        for (std::size_t i = 0; i < line.size(); ++i)
        {
            if (line[i] == '"' && (i == 0 || line[i - 1] != '\\'))
            {
                in_quotes = !in_quotes;
            }
            if (!in_quotes && line[i] == '#')
            {
                return line.substr(0, i);
            }
        }
        return line;
    }

    bool parse_int(const std::string &input, int *value)
    {
        char *end = nullptr;
        const long parsed = std::strtol(input.c_str(), &end, 10);
        if (end == input.c_str() || *end != '\0')
        {
            return false;
        }
        *value = static_cast<int>(parsed);
        return true;
    }

    bool parse_float(const std::string &input, float *value)
    {
        char *end = nullptr;
        const float parsed = std::strtof(input.c_str(), &end);
        if (end == input.c_str() || *end != '\0')
        {
            return false;
        }
        *value = parsed;
        return true;
    }

    std::string unquote(const std::string &input)
    {
        if (input.size() < 2 || input.front() != '"' || input.back() != '"')
        {
            return input;
        }
        std::string out;
        out.reserve(input.size() - 2);
        bool escaped = false;
        for (std::size_t i = 1; i + 1 < input.size(); ++i)
        {
            const char c = input[i];
            if (escaped)
            {
                out.push_back(c);
                escaped = false;
                continue;
            }
            if (c == '\\')
            {
                escaped = true;
                continue;
            }
            out.push_back(c);
        }
        return out;
    }

    bool file_exists(const std::string &path)
    {
        std::ifstream stream(path);
        return stream.good();
    }

    std::string env_or_empty(const char *key)
    {
        const char *value = std::getenv(key);
        if (value == nullptr)
        {
            return "";
        }
        return std::string(value);
    }

    std::string resolve_global_relocation_path()
    {
        const std::string config_root = env_or_empty("SLAM_CONFIG_ROOT");
        if (!config_root.empty())
        {
            const std::string canonical_path = config_root + "/relocalization/global_relocation.sml";
            if (file_exists(canonical_path))
            {
                return canonical_path;
            }
        }

        return "";
    }

    std::vector<std::string> split_legacy_array(const std::string &input)
    {
        std::vector<std::string> tokens;
        std::string current;
        bool in_quotes = false;
        bool escaped = false;
        for (char c : input)
        {
            if (escaped)
            {
                current.push_back(c);
                escaped = false;
                continue;
            }
            if (c == '\\')
            {
                current.push_back(c);
                escaped = true;
                continue;
            }
            if (c == '"')
            {
                in_quotes = !in_quotes;
                current.push_back(c);
                continue;
            }
            if (!in_quotes && c == ',')
            {
                tokens.push_back(trim(current));
                current.clear();
                continue;
            }
            current.push_back(c);
        }
        if (!current.empty() || !tokens.empty())
        {
            tokens.push_back(trim(current));
        }
        return tokens;
    }

    YAML::Node parse_legacy_value(const std::string &raw)
    {
        const std::string value = trim(raw);
        if (value.empty())
        {
            return YAML::Node();
        }

        if (value.front() == '"' && value.back() == '"')
        {
            YAML::Node node;
            node = unquote(value);
            return node;
        }

        if (value.front() == '[' && value.back() == ']')
        {
            YAML::Node sequence(YAML::NodeType::Sequence);
            const std::string inner = trim(value.substr(1, value.size() - 2));
            if (inner.empty())
            {
                return sequence;
            }
            for (const auto &token : split_legacy_array(inner))
            {
                sequence.push_back(parse_legacy_value(token));
            }
            return sequence;
        }

        int int_value = 0;
        if (parse_int(value, &int_value))
        {
            YAML::Node node;
            node = int_value;
            return node;
        }

        float float_value = 0.f;
        if (parse_float(value, &float_value))
        {
            YAML::Node node;
            node = float_value;
            return node;
        }

        YAML::Node node;
        node = value;
        return node;
    }

    bool try_parse_yaml_document(const std::string &doc, YAML::Node *root, std::vector<std::string> *key_order)
    {
        try
        {
            YAML::Node parsed = YAML::Load(doc);
            if (!parsed || !parsed.IsMap())
            {
                return false;
            }
            *root = parsed;
            key_order->clear();
            for (auto it = parsed.begin(); it != parsed.end(); ++it)
            {
                key_order->push_back(it->first.as<std::string>());
            }
            return true;
        }
        catch (const std::exception &)
        {
            return false;
        }
    }

    bool try_parse_legacy_document(const std::string &doc, YAML::Node *root, std::vector<std::string> *key_order)
    {
        YAML::Node parsed(YAML::NodeType::Map);
        std::vector<std::string> keys;
        std::istringstream stream(doc);
        std::string line;
        bool has_value = false;

        while (std::getline(stream, line))
        {
            line = trim(strip_comment(line));
            if (line.empty())
            {
                continue;
            }

            const auto eq = line.find('=');
            if (eq == std::string::npos)
            {
                return false;
            }

            const std::string key = trim(line.substr(0, eq));
            const std::string value = trim(line.substr(eq + 1));
            if (key.empty())
            {
                return false;
            }

            parsed[key] = parse_legacy_value(value);
            keys.push_back(key);
            has_value = true;
        }

        if (!has_value)
        {
            return false;
        }

        *root = parsed;
        *key_order = keys;
        return true;
    }

    bool parse_document_compat(const std::string &doc, YAML::Node *root, std::vector<std::string> *key_order)
    {
        return try_parse_yaml_document(doc, root, key_order) ||
               try_parse_legacy_document(doc, root, key_order);
    }

    std::string escape_string(const std::string &input)
    {
        std::string out;
        out.reserve(input.size());
        for (char c : input)
        {
            if (c == '"' || c == '\\')
            {
                out.push_back('\\');
            }
            out.push_back(c);
        }
        return out;
    }

    std::string emit_legacy_value(const YAML::Node &node)
    {
        if (!node || node.IsNull())
        {
            return "\"\"";
        }

        if (node.IsSequence())
        {
            std::ostringstream out;
            out << "[";
            for (std::size_t i = 0; i < node.size(); ++i)
            {
                if (i != 0)
                {
                    out << ", ";
                }
                out << emit_legacy_value(node[i]);
            }
            out << "]";
            return out.str();
        }

        int int_value = 0;
        if (cyber::detail::from_yaml_node(node, int_value))
        {
            return std::to_string(int_value);
        }

        float float_value = 0.f;
        if (cyber::detail::from_yaml_node(node, float_value))
        {
            std::ostringstream out;
            out.setf(std::ios::fixed, std::ios::floatfield);
            out.precision(6);
            out << float_value;
            return out.str();
        }

        std::string string_value;
        if (cyber::detail::from_yaml_node(node, string_value))
        {
            return "\"" + escape_string(string_value) + "\"";
        }

        YAML::Emitter emitter;
        emitter << node;
        return emitter.c_str();
    }
} // namespace

namespace cyber
{
    sml::sml() : document_(YAML::Node(YAML::NodeType::Map)) {}

    sml::sml(const sml &x) = default;

    sml::sml(sml &&x) = default;

    sml &sml::operator=(const sml &x) = default;

    sml &sml::operator=(sml &&x) = default;

    sml::~sml() = default;

    void sml::remember_key(const std::string &key)
    {
        if (std::find(key_order_.begin(), key_order_.end(), key) == key_order_.end())
        {
            key_order_.push_back(key);
        }
    }

    void sml::rebuild_key_order()
    {
        key_order_.clear();
        if (!document_ || !document_.IsMap())
        {
            return;
        }
        for (auto it = document_.begin(); it != document_.end(); ++it)
        {
            key_order_.push_back(it->first.as<std::string>());
        }
    }

    void sml::clear()
    {
        document_ = YAML::Node(YAML::NodeType::Map);
        key_order_.clear();
    }

    bool sml::del(const std::string &key)
    {
        if (!document_[key])
        {
            return false;
        }

        YAML::Node rebuilt(YAML::NodeType::Map);
        for (auto it = document_.begin(); it != document_.end(); ++it)
        {
            const std::string current_key = it->first.as<std::string>();
            if (current_key == key)
            {
                continue;
            }
            rebuilt[current_key] = it->second;
        }
        document_ = rebuilt;
        key_order_.erase(std::remove(key_order_.begin(), key_order_.end(), key), key_order_.end());
        return true;
    }

    std::string sml::to_string() const
    {
        std::ostringstream out;
        std::vector<std::string> emitted;
        emitted.reserve(key_order_.size());

        for (const auto &key : key_order_)
        {
            const YAML::Node node = document_[key];
            if (!node)
            {
                continue;
            }
            out << key << "=" << emit_legacy_value(node) << "\n";
            emitted.push_back(key);
        }

        for (auto it = document_.begin(); it != document_.end(); ++it)
        {
            const std::string key = it->first.as<std::string>();
            if (std::find(emitted.begin(), emitted.end(), key) != emitted.end())
            {
                continue;
            }
            out << key << "=" << emit_legacy_value(it->second) << "\n";
        }

        return out.str();
    }

    void sml::parse(const std::string &doc)
    {
        YAML::Node parsed(YAML::NodeType::Map);
        std::vector<std::string> key_order;
        if (!parse_document_compat(doc, &parsed, &key_order))
        {
            clear();
            return;
        }
        document_ = parsed;
        key_order_ = key_order;
    }

    void sml::save(const std::string &filename) const
    {
        std::ofstream out(filename);
        if (!out.is_open())
        {
            throw std::runtime_error("failed to open config file for writing: " + filename);
        }
        out << to_string();
    }

    bool sml::load(const std::string &filename)
    {
        std::ifstream in(filename);
        if (!in.is_open())
        {
            return false;
        }

        std::ostringstream buffer;
        buffer << in.rdbuf();

        YAML::Node parsed(YAML::NodeType::Map);
        std::vector<std::string> key_order;
        if (!parse_document_compat(buffer.str(), &parsed, &key_order))
        {
            return false;
        }

        document_ = parsed;
        key_order_ = key_order;
        return true;
    }
} // namespace cyber

namespace sml_config
{
    std::string slam_root;

    cyber::sml sml_global_relocation;
    void fill_global_relocation_default(cyber::sml &sml)
    {
        // peak finder
        sml.put(CONFIG_PEAKFINDER_MIN_VALUE, 0.03f);
        sml.put(CONFIG_PEAKFINDER_MIN_DIFFERENCE, 0.001f);
        // detector
        sml.put(CONFIG_DETECTOR_SCALES, 5);
        sml.put(CONFIG_DETECTOR_SIGMA, 0.2f);
        sml.put(CONFIG_DETECTOR_STEP, 1.4f);
        sml.put(CONFIG_DETECTOR_DMST, 2);
        // describer
        sml.put(CONFIG_DESCRIBER_MIN_RHO, 0.02f);
        sml.put(CONFIG_DESCRIBER_MAX_RHO, 5.0f);
        sml.put(CONFIG_DESCRIBER_BIN_RHO, 4);
        sml.put(CONFIG_DESCRIBER_BIN_PHI, 4);
        // matcher
        sml.put(CONFIG_MATCHER_ACCEPTANCE_THRESHOLD, 0.1f);
        sml.put(CONFIG_MATCHER_SUCCESS_PROBABILITY, 0.95f);
        sml.put(CONFIG_MATCHER_INLIER_PROBABILITY, 0.4f);
        sml.put(CONFIG_MATCHER_DISTANCE_THRESHOLD, 0.4f);
        sml.put(CONFIG_MATCHER_RIGIDITY_THRESHOLD, 0.3f);
        sml.put(CONFIG_MATCHER_ADAPTIVE, 0);
        // movebase
        sml.put(CONFIG_MOVEBASE_CONTROLLER, "RppController");
    }

    void load_global_relocation(const std::string &path)
    {
        auto r = sml_global_relocation.load(path);
        if (!r)
        {
            printf("Load global_relocation.sml failed, use default values.\n");
            fill_global_relocation_default(sml_global_relocation);
            sml_global_relocation.save(path);
        }
    }

    bool sml_init()
    {
        const std::string resolved_path = resolve_global_relocation_path();
        if (resolved_path.empty())
        {
            std::cout << "Cannot resolve global_relocation.sml from `SLAM_CONFIG_ROOT`." << std::endl;
            std::exit(-1);
        }

        const std::string slam_root_env = env_or_empty("SLAM_ROOT");
        if (!slam_root_env.empty())
        {
            slam_root = slam_root_env;
            printf("SLAM_ROOT=%s\n", slam_root.c_str());
        }

        const std::string slam_config_root_env = env_or_empty("SLAM_CONFIG_ROOT");
        if (!slam_config_root_env.empty())
        {
            printf("SLAM_CONFIG_ROOT=%s\n", slam_config_root_env.c_str());
        }

        load_global_relocation(resolved_path);
        return true;
    }

} // namespace sml_config
