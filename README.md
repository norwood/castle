Castle
======

About
-----
Castle is a test harness for Apache Kafka and related projects.

Building Castle
---------------
You can build Castle with Maven 3.  Version 3.3.9 has been confirmed to work, but other Maven versions should work as well.

    mvn install -DskipTests -Dfindbugs.skip

Running Castle on Docker
------------------------
    # Set up the Kafka path.
    export CASTLE_KAFKA_PATH="/home/cmccabe/src/kafka"

    # Bring up a simple 1-node cluster
    ./bin/castle.sh -c ./conf/simple_aws.conf -v -w /tmp/simple up

    # Check the status of the cluster
    ./bin/castle.sh -w /tmp/simple status

Running Castle on AWS
---------------------
    # Set up our AWS information and Kafka path.
    export CASTLE_AWS_KEY="(my AWS key)"
    export CASTLE_AWS_SECURITY_GROUP="(my AWS security group)"
    export CASTLE_SSH_IDENTITY_FILE="(my AWS ssh identity file)"
    export CASTLE_KAFKA_PATH="/home/cmccabe/src/kafka"

    # Bring up a simple 1-node cluster
    ./bin/castle.sh -c ./conf/simple_aws.conf -v -w /tmp/simple up

    # Check the status of the cluster
    ./bin/castle.sh -w /tmp/simple status

Castle Actions
--------------
The castle command-line tool takes one or more "action" arguments.  Actions
specify what the tool should do.  The most important actions are these:

    up:                Bring up all nodes.
      init:            Allocate nodes.
      setup:           Set up all nodes.
      start:           Start the system.

    status:            Get the system status.
      daemonStatus:    Get the status of system daemons.
      taskStatus:      Get the status of trogdor tasks.

    down:              Bring down all nodes.
      saveLogs:        Save the system logs.
      stop:            Stop the system.
      destroy:         Deallocate nodes.

    destroyNodes:      Destroy all nodes.

"up" contains three actions: "init", "setup", and "start".  These can also be
invoked separately, if you want.  Similarly, status and down contain other
actions as well.

By default, actions are applied on all nodes.  If you want to apply an action
only on a single node, you can specify the action as type:scope.
For example, this invocation stops only the broker on node 2:

    ./bin/castle.sh -w /tmp/mycluster stopBroker:node2

Castle Cluster Files
--------------------
A castle cluster file contains three sections: conf, nodes, and roles.

The "conf" section contains miscellaneous configuration strings.  kafkaPath is
the path to the Kafka source directory.  castlePath is the path to the Castle
source directory.  globalTimeout is the number of seconds to wait before timing
out any Castle operation.

The "nodes" section specifies the set of nodes in the cluster.  Each node has a
list of roles describing what the node can do.  Nodes can be specified using
bash-style numeric globs.  For example "node[0-2]" specifies that we should create
three identical nodes: node0, node1, and node2.

The "roles" section maps role names to role configurations.  The configuration
for each role will depend on the role type.  For example, the "broker" type
takes a "jvmOptions" field specifying the JVM options to use when starting the
Kafka broker.

Configuration Metavariables
---------------------------
In a configuration file, a string of the form %{CASTLE_ENV_VARIABLE_NAME} will
be replaced by the value of the environment variable named
CASTLE_ENV_VARIABLE_NAME.  It is an error to try to use a cluster file without
filling out all required environment variables.  Note that the environment
variable name must begin with CASTLE_.

Builtin Metavariables
---------------------
There are other magic strings available.  %{bootstrapServers} will be replaced
by a value calculated based on the broker ports and hostnames.  %{CASTLE_PATH}
always contains a path to the castle source directory.
