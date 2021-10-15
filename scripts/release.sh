#!/bin/bash

set -e

version="$1"
notes_file="$2"

if [ "$version" == "" ]; then
    echo "version is required"
    exit 1
fi

if [ "$notes_file" == "" ]; then
    echo "notes_file is required"
    exit 1
fi

if [ ! -f $notes_file ]; then
    echo "release notes file does not exist"
    exit 1
fi

curr_dir="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
# go to the root dir
cd $curr_dir/..

echo "check version"
grep -q "VERSION := $version" Makefile

if [ ! -z "$(git status --porcelain)" ]; then
    echo "Uncommitted change in work directory"
    exit 1
fi

make clean
make
make image

./scripts/integtest/test_all.sh

git tag -a $version -m "vda version $version"
git push
git push --tags

docker push virtualdiskarray/vdacsi

os_and_arch="linux_amd64"
folder_name="vda_${os_and_arch}_${version}"
zip_name="${folder_name}.zip"
targz_name="${folder_name}.tar.gz"
cp -r _out/$os_and_arch $folder_name
zip -r $zip_name $folder_name
tar zcvf $targz_name $folder_name

gh release create $version $zip_name $targz_name --title "vda $version" --notes-file $notes_file --draft
