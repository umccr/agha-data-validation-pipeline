#!/bin/bash

# Set modified package version
sed -i 's/#define FQTOOLS_VERSION .\+$/#define FQTOOLS_VERSION "'${PKG_VERSION}'"/' ./src/fqheader.h

export C_INCLUDE_PATH=${PREFIX}/include
export LIBRARY_PATH=${PREFIX}/lib

make CC="$CC -fcommon" LIBS="-L$PREFIX/lib -lhts -lz -lm"
mkdir -p $PREFIX/bin
cp bin/fqtools $PREFIX/bin
