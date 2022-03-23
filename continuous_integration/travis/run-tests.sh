set -xe

source activate test-environment
cd ray-yarn
py.test ray_yarn --verbose -s
