/**
 * @file git_version.cpp
 *
 * This file is automatically created by the pre-commit hook that is distributed
 * with Derecho in the githooks folder; this script should be installed in your
 * local repository's .git/hooks/ directory.
 */

#include "derecho/core/git_version.hpp"

namespace derecho {

const int MAJOR_VERSION = 2;
const int MINOR_VERSION = 4;
const int PATCH_VERSION = 1;
const int COMMITS_AHEAD_OF_VERSION = 4;
const char* VERSION_STRING = "2.4.1";
const char* VERSION_STRING_PLUS_COMMITS = "2.4.1+4";

}
