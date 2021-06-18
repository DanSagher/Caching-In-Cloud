import boto3
import requests
from datetime import datetime
from ec2_metadata import ec2_metadata
from flask import Flask, request
from uhashring import HashRing

data_dict = {}
expiration_dict = {}
app = Flask(__name__)

@app.route('/put', methods=['GET', 'POST'])
def put():
    key = request.args.get('strKey')
    data = request.args.get('data')
    expiration_date = request.args.get('expirationDate')

    # Find target node
    healty_nodes_temp = get_healty_instances_id()
    target_node = get_key_node_id(key, healty_nodes_temp)
    target_node_index = healty_nodes_temp.index(target_node)

    healty_nodes = healty_nodes_temp.copy()

    alt_node_index = -1
    alt_node = -1
    # More than one instances
    if (len(healty_nodes) > 1):
        healty_nodes_temp.remove(target_node)

        # Find alternative node
        alt_node = get_key_node_id(key, healty_nodes_temp)
        alt_node_index = healty_nodes.index(alt_node)

    current_node_index = healty_nodes.index(ec2_metadata.instance_id)

    if (target_node_index == current_node_index):
        # This is target node
        store_and_pass(key, data, expiration_date, alt_node)
    elif (alt_node_index == current_node_index):
        # This is alternative node
        store_and_pass(key, data, expiration_date, target_node)
    else:
        pass_data_to_target(key, data, expiration_date, target_node)


    return "", 201

@app.route('/get', methods=['GET'])
def get():
    key = request.args.get('strKey')

    # Find target node
    healty_nodes_temp = get_healty_instances_id()
    target_node = get_key_node_id(key, healty_nodes_temp)
    target_node_index = healty_nodes_temp.index(target_node)

    # Finde alt node
    healty_nodes = healty_nodes_temp.copy()
    alt_node_index = -1
    # More than one instances
    if (len(healty_nodes) > 1):
        healty_nodes_temp.remove(target_node)

        # Find alternative node
        alt_node = get_key_node_id(key, healty_nodes_temp)
        alt_node_index = healty_nodes.index(alt_node)

    current_node_index = healty_nodes.index(ec2_metadata.instance_id)

    if (target_node_index == current_node_index or alt_node_index == current_node_index):
        # get data from current node
        val = str(data_dict.get(key))
        if (val != "None"):
            exp_date = expiration_dict.get(key)
            if (exp_date != "None"):
                try:
                    datetime_object = datetime.strptime(exp_date, '%b-%d-%Y')
                    if (datetime_object > datetime.now()):
                        return val, 201
                    else:
                        data_dict.pop(key, None)
                        expiration_dict.pop(key, None)
                        return "None", 202
                except:
                    print("Could not parse expiration date time.")
                    return val, 201
            else:
                return val, 201


    # get data from target node

    content, code = get_data_from_neighbor(key, target_node)
    if code == 201:
        return content, code

    # get data from alternative node
    content, code = get_data_from_neighbor(key, alt_node)
    if code == 201:
        return content, code

    return "None", 202

@app.route('/getFromInstance', methods=['GET'])
def getFromInstance():
    key = request.args.get('strKey')
    val = str(data_dict.get(key))
    if (val == "None"):
        code = 202
    else:
        exp_date = expiration_dict.get(key)
        if (exp_date != "None"):
            try:
                datetime_object = datetime.strptime(exp_date, '%b-%d-%Y')
                if (datetime_object > datetime.now()):
                    return val, 201
                else:
                    data_dict.pop(key, None)
                    expiration_dict.pop(key, None)
                    return "None", 202
            except:
                print("Could not parse expiration date time.")
                code = 201
    return val, code

@app.route('/healthcheck', methods=['GET', 'POST'])
def health():
    return "bol", 200


@app.route('/putFromNeighbor', methods=['POST'])
def putFromNeighbor():
    key = request.args.get('strKey')
    data = request.args.get('data')
    expiration_date = request.args.get('expirationDate')
    data_dict[key] = data
    expiration_dict[key] = expiration_date

    return "", 201

def get_data_from_neighbor(key, neighbor_id):
    if (neighbor_id != ec2_metadata.instance_id):
        next_dns = get_instance_public_dns(neighbor_id)
        end_point = "http://" + next_dns + "/getFromInstance?strKey=" + key
        response = requests.get(url=end_point)
        return response.content, response.status_code
    else:
        return "None", 202


def get_healty_instances_id():

    elb = boto3.client('elbv2', region_name=ec2_metadata.region)
    lbs = elb.describe_load_balancers()
    num_of_lbs = len(lbs)
    isFound = False

    for i in range(num_of_lbs):
        lb_arn = lbs["LoadBalancers"][i]["LoadBalancerArn"]
        response_tg = elb.describe_target_groups(
            LoadBalancerArn=lb_arn
        )

        num_of_tg = len(response_tg["TargetGroups"])
        for j in range (num_of_tg):
            target_group_arn = response_tg["TargetGroups"][0]["TargetGroupArn"]

            response_health = elb.describe_target_health(
                TargetGroupArn=target_group_arn
            )

            healty_instances = []
            for instance in response_health['TargetHealthDescriptions']:
                if instance['TargetHealth']['State'] == 'healthy':
                    healty_instances.append(instance['Target']['Id'])
                    if (instance['Target']['Id'] == ec2_metadata.instance_id):
                        isFound = True

            if (isFound):
                return healty_instances
    return []

def get_instance_public_dns(instanc_id):
    client = boto3.client('ec2', region_name=ec2_metadata.region)
    response_in = client.describe_instances(
        InstanceIds=[
            str(instanc_id)
        ]
    )

    public_dns_name = response_in['Reservations'][0]['Instances'][0]['PublicDnsName']
    return public_dns_name

def get_key_node_id(key, nodes):
    hr = HashRing(nodes=nodes)
    target_node_id = hr.get_node(key)

    return target_node_id

def store_and_pass(key, data, expiration_date, instance_id):
    data_dict[key] = data
    expiration_dict[key] = expiration_date

    if (instance_id == -1):
        return

    next_dns = get_instance_public_dns(instance_id)
    end_point = "http://" + next_dns + "/putFromNeighbor?strKey=" + key + "&data=" + data + "&expirationDate=" + expiration_date
    requests.post(url=end_point)


def pass_data_to_target(key, data, expiration_date, target_node):
    next_dns = get_instance_public_dns(target_node)

    # send regular put request, not from neighbor
    end_point = "http://" + next_dns + "/put?strKey=" + key + "&data=" + data + "&expirationDate=" + expiration_date
    requests.post(url=end_point)
