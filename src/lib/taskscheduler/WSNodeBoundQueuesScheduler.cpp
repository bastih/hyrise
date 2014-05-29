// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "SharedScheduler.h"
#include "WSNodeBoundQueuesScheduler.h"

namespace hyrise {
namespace taskscheduler {

namespace {
bool registered1 =
    SharedScheduler::registerScheduler<WSNodeBoundPriorityQueuesScheduler>("WSNodeBoundPriorityQueuesScheduler");
bool registered2 = SharedScheduler::registerScheduler<WSNodeBoundBasicQueuesScheduler>("WSNodeBoundQueuesScheduler");
}
}
}
