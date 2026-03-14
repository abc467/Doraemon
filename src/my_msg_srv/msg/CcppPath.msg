int32 id
string name # 路径名
geometry_msgs/Polygon polygon  # 多边形区域 (x, y, 0)
uint8 mode  # 路径类型: 0: 弓字形路径; 1: 回字形路径;
int32 interval # cell间隔决定路径的间距: 一般取值10,可以上下调整[5,15]
int32 free_threshold # 路径接近障碍程度: [1, 254], 值越小路径越远离障碍, 一般取值30
geometry_msgs/Polygon[] paths   # 路径点集合(x, y, theta)