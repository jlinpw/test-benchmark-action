from client import Client
import sys, json
import traceback
import time
import requests

from time import sleep

from client_functions import *

# TODO: make code cleaner (a lot cleaner)

if __name__ == "__main__":
    # input arguments
    pw_user_host = sys.argv[1]  # beluga.parallel.works
    pw_api_key = sys.argv[2]  # echo ${PW_API_KEY}
    user = sys.argv[3]  # echo ${PW_USER}
    resource_name = sys.argv[4]  # Not case sensitive
    resource_type = sys.argv[5]
    wf_name = sys.argv[6]
    wf_xml_args = json.loads(sys.argv[7])

    # initialize the client
    c = Client("https://" + pw_user_host, pw_api_key)

    # Make sure we get to stopping the resources!
    run_workflow = True

    # Exit with error code:
    exit_error = ""

    # Cluster status (can be created or already-made)
    cluster_status = []

    # Creating resources
    try:
        cluster_status.append(create_resource(resource_name, resource_type, c))
    except requests.exceptions.HTTPError as e:
        msg = e.response.text
        printd(msg)
        traceback.print_exc()
        run_workflow = False
        exit_error += msg

    # Configure the cluster using resource.json, but only if it was just created
    my_clusters = c.get_resources()
    cluster = next(
        (item for item in my_clusters if item["name"] == resource_name), None
    )
    cluster_id = cluster["id"]
    if cluster:
        try:
            with open("resource.json") as cluster_definition:
                data = json.load(cluster_definition)
                printd("Updating resource {}".format(resource_name))
                data["resourceName"] = resource_name
                c.update_v2_cluster(cluster_id, data)
                printd("{} updated".format(resource_name))
        except requests.exceptions.HTTPError as e:
            msg = e.response.text
            printd(msg)
            traceback.print_exc()
            if cluster_status[0] == "created":
                c.delete_resource(cluster_id)
            exit_error += msg
            raise Exception(exit_error)
    else:
        printd("{} not found".format(resource_name))

    # Starting resource
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

            if cluster["name"] == resource_name and cluster["status"] == "on":
                
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

    # Deleting the resource
    if cluster_status[0] == "created":
        try:
            printd("Deleting resource {}".format(resource_name))
            c.delete_resource(cluster_id)
            printd("Deleted {} successfully".format(resource_name))
        except requests.exceptions.HTTPError as e:
            msg = e.response.text
            printd(msg)
            traceback.print_exc()
            run_workflow = False
            exit_error += msg

    # If there's any error, delete the resource if it was just created
    if exit_error:
        if cluster_status[0] == "created":
            c.delete_resource(cluster_id)
        raise (Exception(exit_error))
