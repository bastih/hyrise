// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "SharedScheduler.h"
#include "WSThreadLevelQueuesScheduler.h"

namespace hyrise {
namespace taskscheduler {

namespace {
bool registered1 =
    SharedScheduler::registerScheduler<WSThreadLevelPriorityQueuesScheduler>("WSThreadLevelPriorityQueuesScheduler");
bool registered2 =
    SharedScheduler::registerScheduler<WSThreadLevelBasicQueuesScheduler>("WSThreadLevelQueuesScheduler");
}
}
}
