std_msgs/Header header

# A latched snapshot is authoritative only while valid is true and its runtime
# map scope matches the map currently loaded by navigation. An invalid message
# is an explicit tombstone which replaces a previously latched snapshot.
bool valid
string invalid_reason

# Canonical asset identity stored in planning.db.
string map_id
string map_md5

# Runtime identity verified by the publisher for this exact snapshot. A map
# asset may legitimately have a canonical hash different from the OccupancyGrid
# hash produced after loading it, so both identities are carried explicitly.
string map_revision_id
string runtime_map_id
string runtime_map_md5

string constraint_version

coverage_msgs/ZoneGeometry[] no_go_polygons
coverage_msgs/ZoneGeometry[] virtual_wall_keepouts
