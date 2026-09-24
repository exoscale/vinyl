#!/bin/bash
configure_database() {
    if ! fdbcli -C /var/fdb/fdb.cluster --exec status --timeout 1 ; then
        echo "Configuring database"
        until fdbcli -C /var/fdb/fdb.cluster --exec 'configure new single memory; status' --timeout 10 ; do
            sleep 2
        done
        echo "Done configuring database"
    fi

    echo "Database ready for connection"
}

#
# Start the init function in the background and proceed to the main entry
# point. The function will hang until Foundationdb is up, then configure
# the database and exit silently.
#

configure_database &
IP=$(hostname -i|rev|cut -d' ' -f1|rev)
echo "docker:docker@$IP:4500" > /var/fdb/run/fdb.ci.cluster

exec /var/fdb/scripts/fdb.bash "$@"
