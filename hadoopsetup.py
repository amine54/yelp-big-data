import os
import subprocess
import xml.etree.ElementTree as ET


hadoop_home = "/home/ibrahim/hadoop-3.4.1"
java_home = "/usr/lib/jvm/java-17-openjdk-amd64"
json_files_path = "/home/ibrahim/mock_data"


def configure_environment_variables():
    global hadoop_home, java_home
    env = os.environ.copy()
    env["HADOOP_HOME"] = hadoop_home
    env["PATH"] = f"{env['PATH']}:{hadoop_home}/bin:{hadoop_home}/sbin"
    env["JAVA_HOME"] = java_home
    return env


def configure_hadoop_files():
    global hadoop_home
    # Edit core - site.xml
    config_file = f'{hadoop_home}/etc/hadoop/core-site.xml'
    tree = ET.parse(config_file)
    root = tree.getroot()

    property_elem = ET.Element('property')
    name_elem = ET.SubElement(property_elem, 'name')
    name_elem.text = 'fs.defaultFS'
    value_elem = ET.SubElement(property_elem, 'value')
    value_elem.text = 'hdfs://localhost:9000'

    root.append(property_elem)
    tree.write(config_file)

    # Edit hdfs - site.xml
    config_file = f'{hadoop_home}/etc/hadoop/hdfs-site.xml'
    tree = ET.parse(config_file)
    root = tree.getroot()

    properties = [
        {
            "name": "dfs.replication",
            "value": "1"
        },
        {
            "name": "dfs.namenode.name.dir",
            "value": f"file:{hadoop_home}/hdfs/namenode"
        },
        {
            "name": "dfs.datanode.data.dir",
            "value": f"file:{hadoop_home}/hdfs/datanode"
        }
    ]

    for prop in properties:
        property_elem = ET.Element('property')
        name_elem = ET.SubElement(property_elem, 'name')
        name_elem.text = prop["name"]
        value_elem = ET.SubElement(property_elem, 'value')
        value_elem.text = prop["value"]
        root.append(property_elem)

    tree.write(config_file)

    # Create necessary directories
    directories = [
        f"{hadoop_home}/hdfs/namenode",
        f"{hadoop_home}/hdfs/datanode"
    ]

    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
    os.system(f"sudo chown -R $USER:$USER {hadoop_home}")

def format_namenode():
    try:
        subprocess.run(['hdfs', 'namenode', '-format'], check=True)
        print("NameNode formatted successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error formatting NameNode: {e}")

def are_hadoop_services_running():
    pid_files = [
        "/tmp/hadoop-ibrahim-namenode.pid",
        "/tmp/hadoop-ibrahim-datanode.pid",
        "/tmp/hadoop-ibrahim-secondarynamenode.pid"
    ]
    for pid_file in pid_files:
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                # Check if the process with the PID is running
                try:
                    os.kill(pid, 0)
                    return True
                except OSError:
                    # Process is not running, continue checking other PID files
                    continue
            except (ValueError, FileNotFoundError):
                continue
    return False

def start_hadoop_services(env):
    if not are_hadoop_services_running():
        try:
            print("Starting HDFS services with environment:", env)
            subprocess.run(['start-dfs.sh'], check=True, shell=True, env=env)
            print("HDFS services started successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error starting HDFS services: {e}")
    else:
        print("HDFS services are already running.")

def stop_hadoop_services(env):
    try:
        subprocess.run(['stop-dfs.sh'], check=True, shell=True, env=env)
        print("HDFS services stopped successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error stopping HDFS services: {e}")

def create_user_home_directory(env):
    username = os.getlogin()
    user_home_dir = f"/user/{username}"
    try:
        result = subprocess.run(['hdfs', 'dfs', '-test', '-d', user_home_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        if result.returncode != 0:
            subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', user_home_dir], check=True, env=env)
            print(f"Created user home directory {user_home_dir} in HDFS.")
        else:
            print(f"User home directory {user_home_dir} already exists in HDFS.")
    except subprocess.CalledProcessError as e:
        print(f"Error creating user home directory {user_home_dir} in HDFS: {e}")


# Step 10: Create yelp directories in HDFS
def create_yelp_directories(env):
    yelp_sub_dirs = [
        'yelp',
        'yelp/business',
        #'yelp/checkin',
        #'yelp/review',
        #'yelp/tip',
        #'yelp/user'
    ]
    for directory in yelp_sub_dirs:
        try:
            # Check if the directory exists
            result = subprocess.run(['hdfs', 'dfs', '-test', '-d', directory], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            if result.returncode != 0:
                subprocess.run(['hdfs', 'dfs', '-mkdir', directory], check=True, env=env)
                print(f"Created directory {directory} in HDFS.")
            else:
                print(f"Directory {directory} already exists in HDFS.")
        except subprocess.CalledProcessError as e:
            print(f"Error creating directory {directory} in HDFS: {e}")

def upload_json_files(env):
    global json_files_path
    file_mapping = {
        'business.json': 'yelp/business',
        #'yelp_academic_dataset_checkin.json': 'yelp/checkin',
        #'yelp_academic_dataset_review.json': 'yelp/review',
        #'yelp_academic_dataset_tip.json': 'yelp/tip',
        #'yelp_academic_dataset_user.json': 'yelp/user'
    }
    for local_file, hdfs_dir in file_mapping.items():
        local_file_path = os.path.join(json_files_path, local_file)
        try:
            hdfs_file_path = f"{hdfs_dir}/{local_file}"
            # Check if the file exists
            result = subprocess.run(['hdfs', 'dfs', '-test', '-f', hdfs_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            if result.returncode != 0:
                subprocess.run(['hdfs', 'dfs', '-put', local_file_path, hdfs_dir], check=True, env=env)
                print(f"Uploaded {local_file} to {hdfs_dir} in HDFS.")
            else:
                print(f"File {local_file} already exists in {hdfs_dir} in HDFS.")
        except subprocess.CalledProcessError as e:
            print(f"Error uploading {local_file} to {hdfs_dir} in HDFS: {e}")

def delete_hadoop_data_directories():
    global hadoop_home
    data_directories = [
        f"{hadoop_home}/hdfs/namenode",
        f"{hadoop_home}/hdfs/datanode"
    ]
    for directory in data_directories:
        if os.path.exists(directory):
            try:
                os.system(f"sudo rm -rf {directory}")
                print(f"Deleted directory {directory}")
            except OSError as e:
                print(f"Error deleting directory {directory}: {e}")
        else:
            print(f"Directory {directory} does not exist.")


if __name__ == "__main__":
    env = configure_environment_variables()

    #stop_hadoop_services(env)
    #delete_hadoop_data_directories()

    configure_hadoop_files()
    format_namenode()
    start_hadoop_services(env)

    create_user_home_directory(env)
    create_yelp_directories(env)
    upload_json_files(env)
