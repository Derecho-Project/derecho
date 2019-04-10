#pragma once

namespace derecho {

/**
 * The current major version number of the Derecho library, as defined by Git.
 * This is updated when the library is compiled.
 */
extern const int MAJOR_VERSION;
/**
 * The current minor version number of the Derecho library, as defined by Git.
 * This is updated when the library is compiled.
 */
extern const int MINOR_VERSION;
/**
 * The current "patch" (more-minor) version number of the Derecho library, as
 * defined by Git. This is updated when the library is compiled.
 */
extern const int PATCH_VERSION;
/**
 * If the currently-compiled version of the Derecho library is more recent than
 * the last "release" version, this is the number of Git commits by which it is
 * ahead. This is updated when the library is compiled.
 */
extern const int COMMITS_AHEAD_OF_VERSION;

/**
 * A constant C-style string containing the current Derecho library version in
 * dot-separated format, e.g. "1.0.0"
 */
extern const char* VERSION_STRING;

/**
 * A constant C-style string containing the current Derecho library version in
 * dot-separated format, plus the number of Git commits the code is ahead of
 * the last "release" version, separated by a plus sign. E.g. "1.0.0+5"
 */
extern const char* VERSION_STRING_PLUS_COMMITS;

}


