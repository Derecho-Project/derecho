#pragma once

#include <memory>

#include "gms_sst.hpp"
#include "view.hpp"

namespace group {
class ViewManager {
    std::unique_ptr<View> view;
    std::unique_ptr<GMSSST> sst;

public:
    ViewManager();
};
}  // namespace group
