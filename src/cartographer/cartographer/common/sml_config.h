#ifndef SML_CONFIG_H
#define SML_CONFIG_H

/**
 * SML_CONFIG:
 * 这个文件用于读取和保存全局重定位配置。
 * 为了尽量保持外部接口稳定，这里保留 cyber::sml 的调用方式，
 * 但底层实现改为 yaml-cpp，并兼容历史的 key=value .sml 文件格式。
 */

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <exception>
#include <cstdio>
#include <string>
#include <vector>

namespace cyber
{
    typedef struct _SML_VOID
    {
        int placeholder = 0;
    } SML_VOID;

    template <typename T>
    struct _sml_result
    {
        bool ok = false;
        T value{};
    };

    template <typename T>
    using sml_result = struct _sml_result<T>;

    namespace detail
    {
        template <typename T>
        inline YAML::Node to_yaml_node(const T &value)
        {
            YAML::Node node;
            node = value;
            return node;
        }

        inline YAML::Node to_yaml_node(const char *value)
        {
            YAML::Node node;
            node = std::string(value);
            return node;
        }

        inline YAML::Node to_yaml_node(const std::vector<SML_VOID> &)
        {
            return YAML::Node(YAML::NodeType::Sequence);
        }

        template <typename T>
        inline bool from_yaml_node(const YAML::Node &node, T &value)
        {
            try
            {
                value = node.as<T>();
                return true;
            }
            catch (const std::exception &)
            {
                return false;
            }
        }

        inline bool from_yaml_node(const YAML::Node &node, std::vector<SML_VOID> &value)
        {
            if (!node || node.IsNull())
            {
                value.clear();
                return true;
            }
            if (node.IsSequence() && node.size() == 0)
            {
                value.clear();
                return true;
            }
            return false;
        }
    } // namespace detail

    class sml
    {
    public:
        sml();
        sml(const sml &x);
        sml(sml &&x);
        sml &operator=(const sml &x);
        sml &operator=(sml &&x);
        ~sml();

        template <typename T>
        void put(const std::string &key, const T &val)
        {
            remember_key(key);
            document_[key] = detail::to_yaml_node(val);
        }

        void clear();

        bool del(const std::string &key);

        template <typename T>
        sml_result<T> get(const std::string &key) const
        {
            sml_result<T> result;
            const YAML::Node node = document_[key];
            if (!node)
            {
                return result;
            }
            result.ok = detail::from_yaml_node(node, result.value);
            return result;
        }

        std::string to_string() const;

        void parse(const std::string &doc);

        void save(const std::string &filename) const;

        bool load(const std::string &filename);

    private:
        void remember_key(const std::string &key);
        void rebuild_key_order();

        YAML::Node document_;
        std::vector<std::string> key_order_;
    };
} // namespace cyber

namespace sml_config
{
    constexpr const char *CONFIG_PEAKFINDER_MIN_VALUE = "peakfinder_min_value";
    constexpr const char *CONFIG_PEAKFINDER_MIN_DIFFERENCE = "peakfinder_min_difference";

    constexpr const char *CONFIG_DETECTOR_SCALES = "detector_scales";
    constexpr const char *CONFIG_DETECTOR_SIGMA = "detector_sigma";
    constexpr const char *CONFIG_DETECTOR_STEP = "detector_step";
    constexpr const char *CONFIG_DETECTOR_DMST = "detector_dmst";

    constexpr const char *CONFIG_DESCRIBER_MIN_RHO = "describer_min_rho";
    constexpr const char *CONFIG_DESCRIBER_MAX_RHO = "describer_max_rho";
    constexpr const char *CONFIG_DESCRIBER_BIN_RHO = "describer_bin_rho";
    constexpr const char *CONFIG_DESCRIBER_BIN_PHI = "describer_bin_phi";

    constexpr const char *CONFIG_MATCHER_ACCEPTANCE_THRESHOLD = "matcher_acceptance_threshold";
    constexpr const char *CONFIG_MATCHER_SUCCESS_PROBABILITY = "matcher_success_probability";
    constexpr const char *CONFIG_MATCHER_INLIER_PROBABILITY = "matcher_inlier_probability";
    constexpr const char *CONFIG_MATCHER_DISTANCE_THRESHOLD = "matcher_distance_threshold";
    constexpr const char *CONFIG_MATCHER_RIGIDITY_THRESHOLD = "matcher_rigidity_threshold";
    constexpr const char *CONFIG_MATCHER_ADAPTIVE = "matcher_adaptive";

    constexpr const char *CONFIG_MOVEBASE_CONTROLLER = "movebase_controller";

    extern cyber::sml sml_global_relocation;
    bool sml_init();

    template <typename _T>
    _T get_fatal(cyber::sml &sml, const std::string &key)
    {
        cyber::sml_result<_T> r = sml.get<_T>(key);
        if (!r.ok)
        {
            printf("Try to get SML key:%s failed! This is fatal! Please check type and key name!", key.c_str());
            std::exit(-1);
        }
        return r.value;
    }
} // namespace sml_config

#endif
