'''
A job request contains the number of tasks to be
launched and the amount of resource required to execute each task.

The RM Proxy is
responsible for receiving state events (e.g., new task request, task
failure, task completion, etc.) from the RM and routing them to the
scheduler.

The scheduler consists of the packer (Sec. 3.2) and the scaler
(Sec. 3.3). 

The packer decides which tasks get scheduled on which
available instances. 

The scaler determines which and when VM instances should be acquired for the 
cluster as well as when task migrations need to be performed to 
handle task runtime misalignments

The packer and scaler make scheduling and scaling decisions
based on task runtime estimates provided by a Runtime Estimator.
'''

'''
PACKER
The primary objective of Stratus is to minimize
the cloud bill of the VC, which is driven mostly by the amount
of resource-time (e.g., VCore-hours) purchased to complete the
workload. Thus, the packer aims to pack tasks tightly, aligning
remaining runtimes of tasks running on an instance as closely as
possible to each other; otherwise, some tasks will complete faster
than others and some of the instance’s capacity will be wasted.

Queue of pending task requests, where each task request contains
the task’s resource vector (VCores and memory), estimated runtime,
priority, and scheduling constraints (e.g., anti-affinity, hardware
requirements, etc.).

For each instance, Stratus tracks the
amount of resource available on the instance and the remaining
runtimes of each task assigned to the instance (i.e., time required
for the task to complete).

notes: 
-so we can determine the resource-time using endtime-starttime?
-we can retrieve resource times similar to the resource time of currently
running requests
-probably maintain some kind of queue data structure to represent the running 
task requests (contains runtime, core usage, memory)
-instance in this case is machineID? can possibly compute this using co
'''

'''
Algorithm and bins
1. Allocate bins with exponentially increasing size (ith bin is [2^i-1, 2^i) 1, 2, 4, 8, 16 etc)
2. Allocate tasks based on their runtimes into the correct bin
3. Tasks are considered for placement in descending order by runtime, longest first
4. Up packing: In placing a task, the packer first looks at
    instances from the same bin as the task. If multiple instances are
    eligible for scheduling the task, the packer chooses the instance
    with the remaining runtime closest to the runtime of the task. Else, consider
    instances in increasing larger bins. If there is available instances, then 
    allocate to instance with most available resources
    
5. Down-packing: Check decreasingly smaller bins for suitable VM. Finds VM with most available resources

notes: 
- from vmType table, we can obtain the remaining resources (core, memory) by 
    mapping the row's vmTypeID to vm table's vmTypeID to obtain core and memory
- instance in this case refers to vmTypeID(???)
'''

'''
SCALER
When Stratus does not have enough instances to accommodate all
tasks in a scheduling event, it scales out immediately and acquires
new instances for the unscheduled tasks.

It decides on a new
instance to acquire at the end of each iteration, assigns unscheduled
tasks to the instance, and continues until each unscheduled task is
assigned to some new instance.

Candidate task groups. Candidate task groups are constructed
so that the ith group contains the first i tasks in the list sorted in
descending runtime order. The first group contains the longest task,
the second group contains the two longest tasks, and so on. The
scaler continues to build candidate task groups until the aggregate
resource request of the largest task group exceeds that of the largest
allowed instance type.
'''
