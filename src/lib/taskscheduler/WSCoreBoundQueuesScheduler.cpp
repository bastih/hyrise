// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "SharedScheduler.h"
#include "WSCoreBoundQueuesScheduler.h"

namespace hyrise {
namespace taskscheduler {

namespace {
bool registered1 =
    SharedScheduler::registerScheduler<WSCoreBoundPriorityQueuesScheduler>("WSCoreBoundPriorityQueuesScheduler");
bool registered2 = SharedScheduler::registerScheduler<WSCoreBoundBasicQueuesScheduler>("WSCoreBoundQueuesScheduler");
}
}
}
