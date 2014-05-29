// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.

#include "SharedScheduler.h"
#include "ThreadLevelQueue.h"

namespace hyrise {
namespace taskscheduler {

namespace {
bool registered1 = SharedScheduler::registerScheduler<ThreadLevelPriorityQueue>("CentralPriorityScheduler");
bool registered2 = SharedScheduler::registerScheduler<ThreadLevelBasicQueue>("CentralScheduler");
bool registered3 = SharedScheduler::registerScheduler<TBBThreadLevelQueue>("TBBCentralScheduler");
}
}
}
