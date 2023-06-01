import os
import json
import numpy as np
from matplotlib import pyplot as plt

fileDIRList = ['json_GQue_1zed_2cam_intel', 'json_GQue_all_1zed_intel', 'json_GQue_all_intel']
#fileDIRList = fileDIRList[::-1]
distribIntervals = 300

jsonDictList = []# [jsonDict, jsonDict,..., jsonDict]
intervalsList = []
1
for fileDIR in fileDIRList:
    root = ''
    fileNameList = []
    for _root, _dir, _name in os.walk(os.path.join(fileDIR, 'json')):
        root = _root
        fileNameList = _name

    jsonDict = {}
    
    for fileName in fileNameList:
        with open(os.path.join(root, fileName)) as f:
            try:
                jsonDict.update(json.load(f))
            except TypeError as e:
                print(e)
    
    jsonDictList.append(dict(sorted(jsonDict.items(), key=lambda x:x[0])))

for jsonDict in jsonDictList:
    keyList = [float(key) * 1000.0 for key in jsonDict]
    keyList = [keyList[i] - keyList[i - 1] for i in range(1, len(keyList))]
    intervalsList.append(keyList)
    print('size:', len(jsonDict))



plt.figure(0, (16, 9))
plt.title('Intervals Between Samples', size=24)
for name_, intervals in zip(fileDIRList, intervalsList):
    xList = [i for i in range(len(intervals))]
    plt.plot(xList, intervals, label=name_)
    plt.legend()
plt.xlabel('Samples', size=16)
plt.ylabel('Intervals (ms)', size=16)
plt.savefig('json_interval.png')
plt.close()

bound = []
for intervals in intervalsList:
    bound.append(min(intervals))
    bound.append(max(intervals))
bound = [min(bound), max(bound)]

distribX = list(np.linspace(bound[0], bound[1], distribIntervals))
distribHistList = []

for intervals in intervalsList:
    addList = [0 for i in range(distribIntervals)]
    for val in intervals:
        for i, thresh in enumerate(distribX):
            if (val < thresh):
                addList[i - 1] += 1
                break
            elif (val >= bound[1]):
                addList[i] += 1
                break
    distribHistList.append(addList)


plt.figure(1, (16, 9))
plt.title('Intervals Distributions', size=24)
for name_, intervals in zip(fileDIRList, distribHistList):
    plt.plot(distribX, intervals, label=name_)
    plt.legend()
plt.xlabel('Intervals (ms)', size=16)
plt.ylabel('Counts', size=16)
plt.savefig('json_distribution.png')
plt.close()