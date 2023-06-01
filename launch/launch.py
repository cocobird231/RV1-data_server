from launch import LaunchDescription
from launch_ros.actions import Node
from ament_index_python.packages import get_package_share_directory

import os
import yaml
from yaml import load, Loader

def generate_launch_description():
    commonFilePath = os.path.join(get_package_share_directory('cpp_dataserver'), 'launch/common.yaml')
    with open(commonFilePath, 'r') as f:
        data = yaml.load(f, Loader=Loader)
    return LaunchDescription([
        Node(
            package="cpp_dataserver",
            namespace=data['node_prop']['namespace'],
            executable="sub",
            output="screen",
            emulate_tty=True,
            parameters=[
                {
                    "topicScanTime_ms" : data['monitorSetting']['topicScanTime_ms'], 
                    "subscribeMsgPack" : data['monitorSetting']['subscribeMsgPack'], 
                    "zedRGBTopicNameList" : data['monitorSetting']['zedRGBTopicNameList'], 
                    "zedDepthTopicNameList" : data['monitorSetting']['zedDepthTopicNameList'], 
                    "outputFilename" : data['RecordSetting']['outputFilename'], 
                    "samplingStep_ms" : data['RecordSetting']['samplingStep_ms'], 
                    "autoSaveTime_s" : data['RecordSetting']['autoSaveTime_s'], 
                    "recordTime_s" : data['RecordSetting']['recordTime_s'], 
                    "numOfImgSaveTh" : data['RecordSetting']['numOfImgSaveTh'], 
                    "numOfGndSaveTh" : data['RecordSetting']['numOfGndSaveTh'], 
                    "mainNodeName" : data['node_prop']['nodeName'], 
                }
            ]
        )
    ])