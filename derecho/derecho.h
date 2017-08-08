/**
 * @file derecho.h
 * This header file includes everything a client of Derecho needs to be able
 * to set up and use the system. A client should be able to construct and use
 * groups, subgroups, replicated objects, etc. just by including this file.
 * @date Nov 29, 2016
 */

#pragma once

#include "derecho_exception.h"
#include "derecho_ports.h"
#include "group.h"
#include "subgroup_functions.h"
#include "subgroup_info.h"
#include "derecho_internal.h"
#include <mutils-serialization/SerializationSupport.hpp>
