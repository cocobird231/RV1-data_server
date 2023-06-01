import os
import json
from enum import Enum
import numpy as np
from matplotlib import pyplot as plt

import cv2

class MsgType(Enum):
    DEVTYPE_NONE = 0
    DEVTYPE_IDTABLE = 1
    DEVTYPE_JOYSTICK = 2
    DEVTYPE_STEERINGWHEEL = 3
    DEVTYPE_MOTOR = 4
    DEVTYPE_BATTERY = 5
    DEVTYPE_IMU = 6
    DEVTYPE_GPS = 7
    DEVTYPE_ENVIRONMENT = 8
    DEVTYPE_RF = 9
    DEVTYPE_ULTRASONIC = 10
    DEVTYPE_IMAGE = 11
    DEVTYPE_MOTOR_AXLE = 12
    DEVTYPE_MOTOR_TURNING = 13
    DEVTYPE_MOTOR_BRAKING = 14

sourceDIR = 'json_GQue_1zed_2cam_intel'
outVideoFPS = 30.0


root = ''
fileNameList = []
for _root, _dir, _name in os.walk(os.path.join(sourceDIR, 'json')):
    root = _root
    fileNameList = _name

jsonDict = {}
for fileName in fileNameList:
    with open(os.path.join(root, fileName)) as f:
        try:
            jsonDict.update(json.load(f))
        except TypeError as e:
            print(e)


#jsonDict = dict(sorted(jsonDict.items(), key=lambda x:x[0]))
# topicDict = {}
# topicTypeDict = {}
# for timestamp in jsonDict:
#     for topic in jsonDict[timestamp]:
#         if (topicDict.get(topic) == None):
#             topicDict[topic] = {}
#             topicTypeDict[topic] = jsonDict[timestamp][topic]['device_type']
#         msgDict = {}
#         for msg in jsonDict[timestamp][topic]:
#             msgDict[msg] = jsonDict[timestamp][topic][msg]
#         topicDict[topic][float(timestamp)] = msgDict

topicDict = {}# {"topic" : [{ts : msg_dict},..., {ts : msg_dict}],...}
topicTypeDict = {}
for timestamp in jsonDict:
    for topic in jsonDict[timestamp]:
        if (topicDict.get(topic) == None):
            topicDict[topic] = []
            topicTypeDict[topic] = jsonDict[timestamp][topic]['device_type']
        msgDict = {}
        for msg in jsonDict[timestamp][topic]:
            msgDict[msg] = jsonDict[timestamp][topic][msg]
        topicDict[topic].append({float(timestamp) : msgDict})

distArrDict = {}
envArrDict = {}
gpsArrDict = {}
imuArrDict = {}
imgMatDict = {}
imgConfDict = {}
for topic in topicDict:
    try:
        topicDict[topic] = sorted(topicDict[topic], key=lambda x:list(x.keys())[0])
        if (topicTypeDict[topic] == MsgType.DEVTYPE_ENVIRONMENT.value):
            arr = np.ndarray(shape=(len(topicDict[topic]), 4), dtype=float)
            for i, dict_ in enumerate(topicDict[topic]):
                for ts in dict_:
                    val = dict_[ts]
                    arr[i] = np.asarray([ts, val['temperature'], val['relative_humidity'], val['pressure']])
            envArrDict[topic] = arr
        elif (topicTypeDict[topic] == MsgType.DEVTYPE_IMU.value):
            arr = np.ndarray(shape=(len(topicDict[topic]), 11), dtype=float)
            for i, dict_ in enumerate(topicDict[topic]):
                for ts in dict_:
                    val = dict_[ts]
                    arr[i] = np.asarray([ts, *val['orientation'], *val['angular_velocity'], *val['linear_acceleration']])
            imuArrDict[topic] = arr
        elif (topicTypeDict[topic] == MsgType.DEVTYPE_ULTRASONIC.value):
            arr = np.ndarray(shape=(len(topicDict[topic]), 4), dtype=float)
            for i, dict_ in enumerate(topicDict[topic]):
                for ts in dict_:
                    val = dict_[ts]
                    arr[i] = np.asarray([ts, val['min'], val['max'], val['distance']])
            distArrDict[topic] = arr
        elif (topicTypeDict[topic] == MsgType.DEVTYPE_GPS.value):
            arr = np.ndarray(shape=(len(topicDict[topic]), 3), dtype=float)
            for i, dict_ in enumerate(topicDict[topic]):
                for ts in dict_:
                    val = dict_[ts]
                    arr[i] = np.asarray([ts, val['latitude'], val['longitude']])
            gpsArrDict[topic] = arr
        elif (topicTypeDict[topic] == MsgType.DEVTYPE_IMAGE.value):
            imgList = []
            prefilename = '-1'
            for dict_ in topicDict[topic]:
                ts = list(dict_.keys())[0]
                val = dict_[ts]
                if (prefilename != val['filename']):
                    prefilename = val['filename']
                    imgList.append({'filepath' : os.path.join(sourceDIR, val['filename']), 'ts' : ts})

            imgMatDict[topic] = imgList
            ts1 = list(topicDict[topic][0].keys())[0]
            ts2 = list(topicDict[topic][-1].keys())[0]
            imgConfDict[topic] = {"width" : topicDict[topic][0][ts1]["width"], \
                                    "height" : topicDict[topic][0][ts1]["height"], \
                                    "frames" : len(imgMatDict[topic]), \
                                    "ts1" : ts1, "ts2" : ts2}
    except:
        print(topic, i, dict_)

for topic in topicTypeDict:
    print("%s [%s]" %(topic, topicTypeDict[topic]))

outputDIR = sourceDIR + '_visualized'
try:
    os.mkdir(outputDIR)
except:
    pass

################################
#           Print ENV
################################
for i, topic in enumerate(envArrDict):
    tr = envArrDict[topic].T
    plt.figure(0, (16, 16))
    fig, (ax1, ax3) = plt.subplots(2, 1, figsize=(16, 16))
    fig.suptitle(topic, size=32)

    ax1.set_title('temperature/relative_humidity', size=24)
    ax1.plot(tr[0], tr[1], 'b-', label='temp')
    ax1.set_ylabel('Temperature (Celsius)', size=20, color='b')
    

    ax2 = ax1.twinx()
    ax2.plot(tr[0], tr[2] * 100.0, 'g-', label='rh')
    ax2.set_ylabel('Relative_humidity (%)', size=20, color='g')

    ax3.set_title('pressure', size=24)
    ax3.plot(tr[0], tr[3], label='x')
    ax3.set_ylabel('Pressure (mbar)', size=20)
    ax3.set_xlabel('time (s)', size=20)
    ax3.legend()

    plt.savefig(os.path.join(outputDIR, topic.replace('/', '_') + '.png'))
    plt.close()


################################
#           Print IMU
################################
for i, topic in enumerate(imuArrDict):
    tr = imuArrDict[topic].T
    plt.figure(0, (16, 16))
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 16))
    fig.suptitle(topic, size=32)

    ax1.set_title('orientation', size=24)
    ax1.plot(tr[0], tr[1], label='x')
    ax1.plot(tr[0], tr[2], label='y')
    ax1.plot(tr[0], tr[3], label='z')
    ax1.plot(tr[0], tr[4], label='w')
    ax1.set_ylabel('Value (ms)', size=20)
    ax1.legend()

    ax2.set_title('angular_velocity', size=24)
    ax2.plot(tr[0], tr[5], label='x')
    ax2.plot(tr[0], tr[6], label='y')
    ax2.plot(tr[0], tr[7], label='z')
    ax2.set_ylabel('Value (ms)', size=20)
    ax2.legend()

    ax3.set_title('linear_acceleration', size=24)
    ax3.plot(tr[0], tr[8], label='x')
    ax3.plot(tr[0], tr[9], label='y')
    ax3.plot(tr[0], tr[10], label='z')
    ax3.set_ylabel('Value (ms)', size=20)
    ax3.set_xlabel('time (s)', size=20)
    ax3.legend()

    plt.savefig(os.path.join(outputDIR, topic.replace('/', '_') + '.png'))
    plt.close()


################################
#          Print Distance
################################
for i, topic in enumerate(distArrDict):
    tr = distArrDict[topic].T
    plt.figure(0, (16, 16))
    fig, (ax1) = plt.subplots(1, 1, figsize=(16, 16))
    fig.suptitle(topic, size=32)

    ax1.set_title('distance', size=24)
    ax1.plot(tr[0], tr[3], label='dist')
    ax1.set_ylabel('Value (ms)', size=20)
    ax1.set_xlabel('Time (s)', size=20)
    ax1.legend()

    plt.savefig(os.path.join(outputDIR, topic.replace('/', '_') + '.png'))
    plt.close()


################################
#          Print GPS
################################
for i, topic in enumerate(gpsArrDict):
    tr = gpsArrDict[topic].T
    plt.figure(0, (16, 16))
    fig, (ax1) = plt.subplots(1, 1, figsize=(16, 16))
    fig.suptitle(topic, size=32)

    ax1.plot(tr[1], tr[2], '.', label='position')
    ax1.set_ylabel('longitude', size=20)
    ax1.set_xlabel('latitude', size=20)
    ax1.legend()

    plt.savefig(os.path.join(outputDIR, topic.replace('/', '_') + '.png'))
    plt.close()


################################
#          Save Video
################################

for topic in imgMatDict:
    imgSize = (imgConfDict[topic]["width"], imgConfDict[topic]["height"])
    fps = imgConfDict[topic]["frames"] / (imgConfDict[topic]["ts2"] - imgConfDict[topic]["ts1"])
    if (fps < outVideoFPS):
        outVideoFPS = fps
    vw = cv2.VideoWriter(os.path.join(outputDIR, topic.replace('/', '_') + '.avi'), cv2.VideoWriter_fourcc('M','J','P','G'), outVideoFPS, imgSize)
    # st = imgMatDict[topic][0]['ts']
    # vw.write(cv2.imread(imgMatDict[topic][0]['filepath']))
    for img in imgMatDict[topic]:# {filepath, ts}
        # if (img['ts'] - st >= 1 / outVideoFPS):
        #     st = img['ts']
        vw.write(cv2.imread(img['filepath']))
    vw.release()

fileSet = set()
for topic in imgMatDict:
    for img in imgMatDict[topic]:
        fileSet.add(img['filepath'].split('/')[-1])
noFileCnt = 0
for _root, _dir, _name in os.walk(sourceDIR):
    for nm in _name:
        try:
            fileSet.remove(nm)
        except:
            noFileCnt += 1
print(fileSet, len(fileSet))