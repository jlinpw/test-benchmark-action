from client import Client
import sys, json
import traceback
import time
import requests

# TODO: Add configuring cluster and deleting cluster
# FIXME: Wont be able to stop the resource if it was just started!
from time import sleep

from client_functions import *

if __name__ == "__main__":
    pw_user_host = sys.argv[1]  # beluga.parallel.works
    pw_api_key = sys.argv[2]  # echo ${PW_API_KEY}
    user = sys.argv[3]  # echo ${PW_USER}
    resource_name = sys.argv[4]  # Not case sensitive
    resource_type = sys.argv[5]
    wf_name = sys.argv[6]
    wf_xml_args = json.loads(sys.argv[7])

    c = Client("https://" + pw_user_host, pw_api_key)

    # create the cluster
    cluster = create_resource(resource_name, resource_type, c)
    cluster_id = cluster["_id"]

    # configure the cluster
    with open("resource.json") as cluster_definition:
        data = json.load(cluster_definition)
        try:
            printd("Updating cluster {}".format(resource_name))
            updated_cluster = c.update_v2_cluster(cluster_id, data)
        except requests.exceptions.HTTPError as e:
            print(e.response.text)

    # Make sure we get to stopping the resources!
    run_workflow = True

    # Exit with error code:
    exit_error = ""

    # Starting resources
    resource_status = []
    try:
        resource_status.append(start_resource(resource_name, c))
    except Exception as e:
        msg = "ERROR: Unexpected error when starting resource " + resource_name
        printd(msg)
        traceback.print_exc()
        run_workflow = False
        exit_error += msg

    last_state = {}
    started = []
    cluster_hosts = []

    printd("\nWaiting for", resource_name, "to start...")
    while True:
        current_state = c.get_resources()
        for cluster in current_state:
            if cluster["name"] in resource_name and cluster["status"] == "on":
                if cluster["name"] not in started:
                    state = cluster["state"]
                    if cluster["name"] not in last_state:
                        printd(cluster["name"], state)
                        last_state[cluster["name"]] = state
                    elif last_state[cluster["name"]] != state:
                        print(cluster["name"], state)
                        last_state[cluster["name"]] = state
                    if "masterNode" in cluster["state"]:
                        if cluster["state"]["masterNode"] != None:
                            ip = cluster["state"]["masterNode"]
                            entry = " ".join([cluster["name"], ip])
                            print(entry)
                            cluster_hosts.append(entry)
                            started.append(cluster["name"])
        if len(started) == 1:
            print("\nCluster started")
            break

        time.sleep(5)

    # add startCmd to wf_xml_args
    startCmd = get_cmd(wf_name, c)
    wf_xml_args["startCmd"] = startCmd
    # TODO: use new cluster id instead of manual input
    wf_xml_args["resource_1"]["id"] = cluster_id

    # Running workflow
    if run_workflow:
        if "not-found" in resource_status:
            msg = "ERROR: Some resources were not found"
            printd(msg)
            run_workflow = False
            exit_error += "\n" + msg

    if run_workflow:
        try:
            # Launching workflow
            response = launch_workflow(wf_name, wf_xml_args, user, c)
            # Waiting for workflow to complete
            state = wait_workflow(wf_name, c)
            if state != "completed":
                msg = "Workflow final state is " + state
                printd(msg)
                exit_error += "\n" + msg
        except Exception:
            msg = "Workflow launch failed unexpectedly"
            printd(msg)
            traceback.print_exc()
            exit_error += "\n" + msg
    else:
        msg = "Aborting workflow launch"
        printd(msg)
        exit_error += "\n" + msg

    # Stoping resources
    sleep(5)
    printd(resource_name, "status", resource_status[0])
    # stop the pool
    stop_resource(resource_name, c)

    # delete the resource
    c.delete_resource(cluster_id)

    if exit_error:
        raise (Exception(exit_error))
