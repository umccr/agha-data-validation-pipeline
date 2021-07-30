#!/bin/bash
set -euo pipefail

# Change into build directory
echo "building layer: ${1}"
cd "${1}"
# Set lib directory to build package (according to AWS Lambda guidelines)
export PKG_DIR='python'
PKG_NAME=$(basename $(pwd -P))
rm -rf ${PKG_DIR} && mkdir -p ${PKG_DIR}
# Install the python libraries (without dependencies) using Docker
# Mount the current working directory as '/foo/' and start container in that directory
docker run \
  --rm \
  -v $(pwd):/foo \
  -w /foo \
  lambci/lambda:build-python3.8 \
  pip install -r requirements.txt --no-deps -t ${PKG_DIR} 1>/dev/null
# Clean the lib directory
rm -rf ${PKG_DIR}/*.dist-info
find ${PKG_DIR} -type d -name '__pycache__' -exec rm -rf {} +
# Create the package zip
zip -r "python38-${PKG_NAME}.zip" ./${PKG_DIR}/ 1>/dev/null
# Remove the inflated directory
rm -rf ${PKG_DIR}/
