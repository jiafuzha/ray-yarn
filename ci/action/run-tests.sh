set -xe

_script="$(readlink -f ${BASH_SOURCE[0]})"
_mydir="$(dirname $_script)"
source $_mydir/load_testuser.sh

source activate test-environment
cd ray-yarn

pytest ray_yarn -s -v 
