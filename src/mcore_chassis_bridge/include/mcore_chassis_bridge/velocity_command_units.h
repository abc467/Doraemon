#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

namespace mcore_chassis_bridge {

inline double ClampAbs(double value, double max_abs) {
  const double limit = std::fabs(max_abs);
  if (limit <= 0.0) {
    return value;
  }
  return std::max(-limit, std::min(limit, value));
}

inline bool IsUnitSign(double velocity_sign) {
  return std::isfinite(velocity_sign) &&
         (velocity_sign == -1.0 || velocity_sign == 1.0);
}

inline bool IsValidVelocityConversionConfig(double velocity_sign,
                                            double max_abs_si,
                                            double protocol_scale) {
  return IsUnitSign(velocity_sign) && std::isfinite(max_abs_si) &&
         max_abs_si >= 0.0 && std::isfinite(protocol_scale) &&
         protocol_scale > 0.0 &&
         (max_abs_si == 0.0 ||
          max_abs_si <=
              static_cast<double>(std::numeric_limits<float>::max()) / protocol_scale);
}

// cmd_vel and max_abs_si are SI values. The M-core protocol scale is applied
// only after the signed SI command has been limited, so protocol units never
// get compared with an SI limit.
inline double ToProtocolVelocity(double velocity_si,
                                 double velocity_sign,
                                 double max_abs_si,
                                 double protocol_scale) {
  if (!std::isfinite(velocity_si) ||
      !IsValidVelocityConversionConfig(velocity_sign, max_abs_si, protocol_scale)) {
    throw std::invalid_argument("invalid M-core velocity conversion input");
  }
  const double limited_si = ClampAbs(velocity_si * velocity_sign, max_abs_si);
  const double protocol_velocity = limited_si * protocol_scale;
  if (!std::isfinite(protocol_velocity) ||
      std::fabs(protocol_velocity) >
          static_cast<double>(std::numeric_limits<float>::max())) {
    throw std::overflow_error("M-core protocol velocity exceeds float range");
  }
  return protocol_velocity;
}

}  // namespace mcore_chassis_bridge
