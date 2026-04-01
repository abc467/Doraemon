uint8 battery_percentage  # 电量百分比
uint16 battery_voltage     # 电池电压（单位：mV）
uint8 sewage_level  # 污水水位（0~100）
uint8 clean_level   # 清水水位（0~100）
uint8 brush_position  # 刷盘位置 (0:原位，1:到位, 2:运动中)
uint8 scraper_position # 刮耙位置（0:原位，1:到位, 2:运动中)
bool[8] obstacle_status  # 超声波避障状态
bool[3] region  # 前扫描仪区域状态
bool[8] status  # 1急停1（按下为1），2急停2，3前进，4后退， 5复位，6抱闸，7供电反馈，8雷达就绪（就绪为1）
