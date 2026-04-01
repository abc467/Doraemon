int32 id
string name # 示教路径名
geometry_msgs/Polygon path  # 原始示教路径


# uint8 type  # 示教类型: 0-原始路径; 1-覆盖路径; 2-原始路径+覆盖路径
# ###### 下述数据始终自动生成，只有在type=1或2时才使用
# uint8 coverage_type     # 覆盖类型: 0-弓字形; 1-回字形
# int32 interval          # cell间隔决定路径的间距: 一般取值10,可以上下调整
# int32 free_threshold    # 路径接近障碍程度: [1, 254], 值越小路径越远离障碍, 一般取值30
# geometry_msgs/Polygon[] paths # 覆盖路径
