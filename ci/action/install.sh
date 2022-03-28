#!/usr/bin/env bash
set -xe

export HOME=/home/testuser
cd $HOME

if [ -f .bash_profile ]; then
  source .bash_profile
fi

conda config --set always_yes yes --set changeps1 no
conda update conda -n base

conda create -n test-environment -c conda-forge \
    ray-default \
    pytest \
    python=$1 \
    pyyaml \
    conda-pack>=0.6 \
    skein>=0.8.1 \

source activate test-environment

cd ~/ray-yarn
python -m pip install -v --no-deps .

conda list

set +xe
