set -xe

source activate test-environment
cd ray-yarn

pytest ray_yarn -s -v 
