proc shutdown_how {srv_id how} {
    if {$how == "shutdown"} {
        catch {R $srv_id shutdown nosave failover}
    } elseif {$how == "sigterm"} {
        exec kill -SIGTERM [s -$srv_id process_id]
    }
}

# We will start a cluster with 3 primary nodes and 4 replicas, the primary 1 will have 2 replicas.
# We will pause the replica 1, and then shutdown the primary 1, and making replica 2 to become
# the new primary.
proc test_auto_failover {how shutdown_timeout} {
    test "Failover on shutdown hands over primaryship to a fully sync'd replica - $how - shutdown-timeout: $shutdown_timeout" {
        set primary [srv 0 client]
        set replica1 [srv -3 client]
        set replica1_pid [s -3 process_id]
        set replica2 [srv -6 client]
        set replica2_host [srv -6 host]
        set replica2_port [srv -6 port]

        if {$how == "sigterm"} {
            $primary config set shutdown-on-sigterm failover
        }
        $primary config set shutdown-timeout $shutdown_timeout
        $primary config set repl-ping-replica-period 3600

        # Pause a replica so it has no chance to catch up with the offset.
        pause_process $replica1_pid

        # Primary write some data to increase the offset.
        for {set i 0} {$i < 10} {incr i} {
            $primary incr key_991803
        }

        if {$shutdown_timeout == 0} {
            # Wait the replica2 catch up with the offset
            wait_for_ofs_sync $primary $replica2
            wait_replica_acked_ofs $primary $replica2 $replica2_host $replica2_port
        } else {
            # If shutdown-timeout is enable, we expect the primary to pause writing
            # and wait for the replica to catch up with the offset.
        }

        # Shutdown the primary.
        shutdown_how 0 $how

        # Wait for the replica2 to become a primary.
        wait_for_condition 1000 50 {
            [s -6 role] eq {master}
        } else {
            puts "s -6 role: [s -6 role]"
            fail "Failover does not happened"
        }

        # Make sure that the expected logs are printed.
        verify_log_message -6 "*Forced failover primary request accepted*" 0

        resume_process $replica1_pid
    }

    test "Unable to find a replica to perform an auto failover - $how" {
        set primary [srv -6 client]
        set replica1 [srv -3 client]
        set replica1_pid [s -3 process_id]

        pause_process $replica1_pid

        if {$how == "sigterm"} {
            $primary config set shutdown-on-sigterm failover
        }
        $primary client kill type replica
        shutdown_how 6 $how
        wait_for_log_messages -6 {"*Unable to find a replica to perform the auto failover on shutdown*"} 0 1000 10

        resume_process $replica1_pid
    }
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_auto_failover "shutdown" 0
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_auto_failover "sigterm" 0
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_auto_failover "shutdown" 10
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_auto_failover "sigterm" 10
}

# Replica 3 is eligible unless disabled or lagging; replica 6 is always nofailover.
proc test_shutdown_respects_nofailover {how shutdown_timeout eligible_state} {
    test "Shutdown respects nofailover - $how - shutdown-timeout: $shutdown_timeout - eligible replica: $eligible_state" {
        set primary [srv 0 client]
        set eligible [srv -3 client]
        set excluded [srv -6 client]
        set excluded_id [$excluded cluster myid]
        set eligible_id [$eligible cluster myid]

        if {$how ne "shutdown"} {
            $primary config set shutdown-on-$how failover
        }
        $primary config set shutdown-timeout $shutdown_timeout
        $excluded config set cluster-replica-no-failover yes
        if {$eligible_state eq "disabled"} {
            $eligible config set cluster-replica-no-failover yes
        }
        wait_for_condition 1000 50 {
            [cluster_has_flag [cluster_get_node_by_id 0 $excluded_id] nofailover] &&
            ($eligible_state ne "disabled" ||
             [cluster_has_flag [cluster_get_node_by_id 0 $eligible_id] nofailover])
        } else {
            fail "Primary did not receive the nofailover flags"
        }

        # Establish the eligible connection first, then reconnect the excluded
        # replica so the old selection loop would pick it as its last match.
        wait_replica_acked_ofs $primary $eligible [srv -3 host] [srv -3 port]
        $excluded client kill type master
        wait_for_condition 1000 50 {
            [s -6 master_link_status] eq "up"
        } else {
            fail "Excluded replica did not reconnect"
        }
        if {$eligible_state eq "lagging"} {
            pause_process [srv -3 pid]
        }
        for {set i 0} {$i < 10} {incr i} {
            $primary incr key_991803
        }
        if {$eligible_state ne "lagging"} {
            wait_replica_acked_ofs $primary $eligible [srv -3 host] [srv -3 port]
        }
        wait_replica_acked_ofs $primary $excluded [srv -6 host] [srv -6 port]
        $primary config set repl-ping-replica-period 3600
        wait_for_condition 1000 50 {
            [check_replica_acked_ofs $primary [srv -6 host] [srv -6 port]] &&
            ($eligible_state eq "lagging" ||
             [check_replica_acked_ofs $primary [srv -3 host] [srv -3 port]])
        } else {
            fail "Replicas did not acknowledge the final offset"
        }

        shutdown_how 0 $how

        if {$eligible_state eq "online"} {
            wait_for_condition 1000 50 {
                [s -3 role] eq "master"
            } else {
                fail "Eligible replica was not promoted"
            }
            verify_log_message -3 "*Forced failover primary request accepted*" 0
        } else {
            wait_for_log_messages 0 {"*Unable to find a replica to perform the auto failover on shutdown*"} 0 1000 50
            assert_equal 0 [count_log_message 0 "*Perform auto failover to replica*"]
            if {$eligible_state eq "lagging"} {
                resume_process [srv -3 pid]
            } else {
                assert_equal "slave" [s -3 role]
            }
        }
        assert_equal "slave" [s -6 role]
        assert_equal 0 [count_log_message -6 "*Forced failover primary request accepted*"]
    }
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_shutdown_respects_nofailover shutdown 0 online
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_shutdown_respects_nofailover sigterm 0 disabled
}

start_cluster 3 4 {tags {external:skip cluster}} {
    test_shutdown_respects_nofailover sigterm 10 lagging
}

start_cluster 3 1 {tags {external:skip cluster}} {
    test "Nofailover rejects an internal shutdown handoff but permits explicit manual failover" {
        set primary [srv 0 client]
        set replica [srv -3 client]
        $replica config set cluster-replica-no-failover yes
        wait_replica_acked_ofs $primary $replica [srv -3 host] [srv -3 port]

        # Inject the exact internal request to model an unpatched primary
        # or one that has not received the replica's latest nofailover flag.
        $primary debug replicate CLUSTER FAILOVER FORCE REPLICAID [$replica cluster myid]
        wait_for_log_messages -3 {"*Ignoring shutdown failover request because cluster-replica-no-failover is set to yes*"} 0 1000 10
        assert_equal "slave" [s -3 role]
        assert_equal 0 [count_log_message -3 "*Forced failover primary request accepted*"]

        # Ignoring the handoff must not interrupt the replication stream.
        $primary set key_991803 after-ignored-handoff
        wait_for_ofs_sync $primary $replica
        $replica readonly
        assert_equal "after-ignored-handoff" [$replica get key_991803]

        $replica cluster failover
        wait_for_condition 1000 50 {
            [s -3 role] eq "master" && [s 0 role] eq "slave"
        } else {
            fail "Explicit failover did not override nofailover"
        }
    }
}
