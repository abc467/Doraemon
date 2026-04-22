from distutils.core import setup
from catkin_pkg.python_setup import generate_distutils_setup

d = generate_distutils_setup(
    packages=[
        'coverage_planner',
        'coverage_planner.coverage_planner_core',
        'coverage_planner.ops_store',
        'coverage_planner.plan_store',
        'coverage_planner.coverage_planner_ros',
        'coverage_planner.slam_workflow',
    ],
    package_dir={'': 'src'},
)

setup(**d)
