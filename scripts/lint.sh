#!/bin/bash

exitcode=0

# Run cpplint
find src \( -iname "*.cpp" -o -iname "*.hpp" \) -a -not -path "src/lib/utils/boost_curry_override.hpp" -a -not -path "src/lib/utils/boost_bimap_core_override.hpp" -print0 | parallel --null --no-notice -j 100% --nice 17 /usr/bin/env python3 ./third_party/cpplint/cpplint.py --verbose=0 --extensions=hpp,cpp --counting=detailed --filter=-legal/copyright,-whitespace/newline,-runtime/references,-build/c++11,-build/include_what_you_use,-readability/nolint,-whitespace/braces,-build/include_subdir --linelength=120 {} 2\>\&1 \| grep -v \'\^Done processing\' \| grep -v \'\^Total errors found: 0\' \; test \${PIPESTATUS[0]} -eq 0
let "exitcode |= $?"
#                             /------------------ runs in parallel -------------------\
# Conceptual: find | parallel python cpplint \| grep -v \| test \${PIPESTATUS[0]} -eq 0
#             ^      ^        ^                 ^          ^
#             |      |        |                 |          |
#             |      |        |                 |          Get the return code of the first pipeline item (here: cpplint)
#             |      |        |                 Removes the prints for files without errors
#             |      |        Regular call of cpplint with options
#             |      Runs the following in parallel
#             Finds all .cpp and .hpp files, separated by \0

# All disabled tests should have an issue number
output=$(grep -rHn 'DISABLED_' src/test | grep -v '#[0-9]\+' | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/\1  Disabled tests should be documented with their issue number (e.g. \/* #123 *\/)/')
if [ ! -z "$output" ]; then
	echo "$output"
	exitcode=1
fi

# Gtest's TEST() should not be used. Use TEST_F() instead. This might require additional test classes but ensures that state is cleaned up properly.
output=$(grep -rHn '^TEST(' src/test | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/\1  TEST() should not be used as it does not clean up global state (e.g., the Hyrise singleton)./')
if [ ! -z "$output" ]; then
	echo "$output"
	exitcode=1
fi

# Tests should inherit from BaseTest or BaseTestWithParams of base_test.hpp to ensure proper destruction.
output=$(grep -rHEn ':[[:space:]]*(public|protected|private)?[[:space:]]+::testing::Test' src/test | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/\1  Tests should inherit from BaseTest\/BaseTestWithParams to ensure a proper clean up./')
if [ ! -z "$output" ]; then
	echo "$output"
	exitcode=1
fi

# The singleton pattern should not be manually implemented
output=$(grep -rHn 'static[^:]*instance;' --exclude singleton.hpp src | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/\1  Singletons should not be implemented manually. Have a look at src\/lib\/utils\/singleton.hpp/')
if [ ! -z "$output" ]; then
	echo "$output"
	exitcode=1
fi

# Tests should not include any type of random behavior, see #1321.
output=$(grep -rHEn '#include <random>|std::random|std::[a-z_]+_engine|std::[a-z_]+_distribution' src/test | grep -v std::random_access_iterator_tag | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/\1  Tests should not include any type of random behavior, see #1321./')
if [ ! -z "$output" ]; then
	echo "$output"
	exitcode=1
fi

# Check for included cpp files. You would think that this is not necessary, but history proves you wrong.
regex='#include .*\.cpp'
namecheck=$(find src \( -iname "*.cpp" -o -iname "*.hpp" \) -print0 | xargs -0 grep -rHn "$regex" | grep -v NOLINT)
let "exitcode |= ! $?"
while IFS= read -r line
do
	if [ ! -z "$line" ]; then
		echo $line | sed 's/^\([a-zA-Z/._]*:[0-9]*\).*/Include of cpp file:/' | tr '\n' ' '
		echo $line | sed 's/\(:[^:]*:\)/\1 /'
	fi
done <<< "$namecheck"

# Check that all cpp and hpp files in src/ are listed in the corresponding CMakeLists.txt
for dir in src/*
do
	for file in $(find $dir -name *.cpp -o -name *.hpp)
	do
		if grep $(basename $file) $dir/CMakeLists.txt | grep -v '#' > /dev/null
		then
			continue
		else
			echo $file not found in $dir/CMakeLists.txt
			exitcode=1
		fi
	done
done

# Check for Windows-style line endings
output=$(find src -type f -exec dos2unix -ic {} +)
if [[ ${output} ]]; then
	for file in ${output}
	do
		echo "Windows-style file ending: $file"
	done
	exitcode=1
fi

# Python linting
output=$(flake8 --max-line-length 120 --extend-ignore=E203 scripts)
if [ ! -z "$output" ]; then
	echo "$output"
	exitcode=1
fi

exit $exitcode
