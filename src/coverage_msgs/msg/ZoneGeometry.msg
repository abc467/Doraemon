# Zone geometry expressed in a frame (usually map)
string frame_id

# outer boundary ring
coverage_msgs/Polygon2D outer

# holes (0..N)
coverage_msgs/Polygon2D[] holes
