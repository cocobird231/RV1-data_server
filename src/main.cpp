#include "header.h"
#include <rcl_interfaces/msg/set_parameters_result.hpp>
#include "vehicle_interfaces/params.h"

#define TS_MODE

#ifdef TS_MODE
class ScanTopicNode : public TimeSyncNode
#else
class ScanTopicNode : public rclcpp::Node
#endif
{
private:
    // ScanTopicNode parameters
    float topicScanTime_ms = 1000.0;
    std::string subscribeMsgPack = "vehicle_interfaces";
    std::vector<std::string> zedRGBTopicNameList;
    std::vector<std::string> zedDepthTopicNameList;

    std::string outputFilename = "datalog";
    double samplingStep_ms = 10.0;
    double autoSaveTime_s = 10.0;
    double recordTime_s = -1.0;
    int numOfImgSaveTh = 4;
    int numOfGndSaveTh = 1;
    bool enabled_record = false;
    rclcpp::Node::OnSetParametersCallbackHandle::SharedPtr paramsCallbackHandler;
    std::mutex externalControlLock_;

    // Stop ScanTopicNode
    bool exitF_;

    // Grab nodes and topics relation table form NodeTopicTable.json
    // {"node_name" : {"topic_name" : "msg_type", ...}, ...}
    NodeTopicMsgTable nodeTopicMsgTable_;
    bool nodeDetectF_;

    // Store ROS2 node names
    NodeContainerPack nodeContainerPack_;// Store node name and topic type
    std::mutex nodeContainerPackLock_;// Prevent nodeContainerPack_ races

    // Store ROS2 topic inforamtions
    TopicContainerPack topicContainerPack_;// Store topic name and message type
    std::mutex topicContainerPackLock_;// Prevent topicContainerPack_ races



    // Store subscriber threads
    std::map<std::string, rclcpp::executors::SingleThreadedExecutor*> execMap_;
    std::map<std::string, std::thread> subThMap_;

    // Store topic messages to dump into files
    OutPack pack_;// Main buffer
    OutPack packBk_;// Backup buffer
    OutPack* packPtr_;// Pointer to buffer

    bool outPackBkF_;// Double buffer flag
    std::mutex outPackPtrLock_;// Prevent packPtr_ races

    std::atomic<double> outFileTimestamp_;// File name in timestamp

    // Save image queue, pass into subscribe node which contains Image message type
    SaveImgQueue* globalImgQue_;
    SaveQueue<WriteGroundDetectStruct>* globalGndQue_;
    
    // Timer definitions
    Timer* monitorTimer_;// Scan topics on lan
    Timer* sampleTimer_;// Grab topic messages at fixed rate
    Timer* dumpTimer_;// Dump packPtr_ buffer into file
    Timer* recordTimer_;// Total execute time.

    bool stopMonitorF_;// Stop monitor timer flag
    
    
private:
    void _getParams()
    {
        this->get_parameter("topicScanTime_ms", this->topicScanTime_ms);
        this->get_parameter("subscribeMsgPack", this->subscribeMsgPack);
        this->get_parameter("zedRGBTopicNameList", this->zedRGBTopicNameList);
        this->get_parameter("zedDepthTopicNameList", this->zedDepthTopicNameList);
        this->get_parameter("outputFilename", this->outputFilename);
        this->get_parameter("samplingStep_ms", this->samplingStep_ms);
        this->get_parameter("autoSaveTime_s", this->autoSaveTime_s);
        this->get_parameter("recordTime_s", this->recordTime_s);
        this->get_parameter("numOfImgSaveTh", this->numOfImgSaveTh);
        this->get_parameter("numOfGndSaveTh", this->numOfGndSaveTh);
        this->get_parameter("enabled_record", this->enabled_record);
    }

    rcl_interfaces::msg::SetParametersResult _paramsCallback(const std::vector<rclcpp::Parameter>& params)
    {
        rcl_interfaces::msg::SetParametersResult res;
        res.successful = true;
        res.reason = "";

        for (const auto& param : params)
        {
            try
            {
                if (param.get_name() == "enabled_record")
                {
                    this->enabled_record = param.as_bool();
                    if (this->enabled_record)
                        this->startSampling();
                    else
                        this->stopSampling();
                    return res;
                }
                else if (param.get_name() == "samplingStep_ms")
                {
                    this->samplingStep_ms = param.as_double();
                    this->sampleTimer_->setInterval(this->samplingStep_ms);
                }
                else if (param.get_name() == "autoSaveTime_s")
                {
                    this->autoSaveTime_s = param.as_double();
                    this->dumpTimer_->setInterval(this->autoSaveTime_s * 1000.0);
                }
                else if (param.get_name() == "recordTime_s")
                {
                    this->recordTime_s = param.as_double();
                    if (this->recordTimer_ == nullptr)
                    {
                        this->recordTimer_ = new Timer(this->recordTime_s * 1000, std::bind(&ScanTopicNode::_recordTimerCallback, this));
                    }
                        
                    else
                        this->recordTimer_->setInterval(this->recordTime_s * 1000.0);
                }
            }
            catch (...)
            {
                res.successful = false;
                res.reason = "[ScanTopicNode::_paramsCallback] Caught Unknown Exception!";
            }
        }

        return res;
    }

    void _monitorTimerCallback()
    {
        // Search topics
        auto map = this->get_topic_names_and_types();
        std::unique_lock<std::mutex> topicContainerLocker(this->topicContainerPackLock_, std::defer_lock);
        topicContainerLocker.lock();
        for (const auto& i : map)
        {
            if (this->topicContainerPack_.find(i.first) == this->topicContainerPack_.end())
            {
                TopicContainer container;
                container.topicName = i.first;
                container.msgType = i.second.back();
                container.occupyF = false;
                container.node = nullptr;
                this->topicContainerPack_[i.first] = container;
                printf("New key [%s]: <%s, %d>\n", i.first.c_str(), container.msgType.c_str(), container.occupyF);
                if (this->stopMonitorF_)
                {
                    topicContainerLocker.unlock();
                    return;
                }
            }
        }
        topicContainerLocker.unlock();

        topicContainerLocker.lock();
        for (auto& i : this->topicContainerPack_)
        {
            if (!i.second.occupyF)
            {
                auto msgTypeSplit = split(i.second.msgType, "/");
                if (msgTypeSplit[0] == this->subscribeMsgPack && msgTypeSplit[1] == "msg")
                {
                    auto splitTopicName = split(i.first, "/");
                    std::string subNodeName = splitTopicName.back() + "_subnode";
                    replace_all(subNodeName, "/", "_");

                    if (msgTypeSplit[2] == "Distance")
                        i.second.node = std::make_shared<DistanceSubNode>(subNodeName, i.first);
                    else if (msgTypeSplit[2] == "Environment")
                        i.second.node = std::make_shared<EnvironmentSubNode>(subNodeName, i.first);
                    else if (msgTypeSplit[2] == "GPS")
                        i.second.node = std::make_shared<GPSSubNode>(subNodeName, i.first);
                    else if (msgTypeSplit[2] == "GroundDetect")
                        i.second.node = std::make_shared<GroundDetectSubNode>(subNodeName, i.first, this->outputFilename, this->globalGndQue_);
                    else if (msgTypeSplit[2] == "IDTable")
                        i.second.node = std::make_shared<IDTableSubNode>(subNodeName, i.first);
                    else if (msgTypeSplit[2] == "Image")
                        i.second.node = std::make_shared<ImageSubNode>(subNodeName, i.first, this->outputFilename, this->globalImgQue_);
                    else if (msgTypeSplit[2] == "IMU")
                        i.second.node = std::make_shared<IMUSubNode>(subNodeName, i.first);
                    else if (msgTypeSplit[2] == "MillitBrakeMotor")
                        i.second.node = std::make_shared<MillitBrakeMotorSubNode>(subNodeName, i.first);
                    else if (msgTypeSplit[2] == "MillitPowerMotor")
                        i.second.node = std::make_shared<MillitPowerMotorSubNode>(subNodeName, i.first);
                    else if (msgTypeSplit[2] == "MotorAxle")
                        i.second.node = std::make_shared<MotorAxleSubNode>(subNodeName, i.first);
                    else if (msgTypeSplit[2] == "MotorSteering")
                        i.second.node = std::make_shared<MotorSteeringSubNode>(subNodeName, i.first);
                    else if (msgTypeSplit[2] == "UPS")
                        i.second.node = std::make_shared<UPSSubNode>(subNodeName, i.first);
                    else if (msgTypeSplit[2] == "WheelState")
                        i.second.node = std::make_shared<WheelStateSubNode>(subNodeName, i.first);
                    else
                        continue;
                    
                    i.second.node->setOffset(this->getCorrectDuration(), this->getTimestampType());

                    this->execMap_[i.first] = new rclcpp::executors::SingleThreadedExecutor();
                    this->execMap_[i.first]->add_node(i.second.node);
                    this->subThMap_[i.first] = std::thread(SpinTopicRecordNodeExecutor, this->execMap_[i.first], i.second.node, i.first);
                    
                    printf("Subscribed [%s][%s]\n", i.first.c_str(), msgTypeSplit[2].c_str());
                    i.second.occupyF = true;
                }
                // ZED preserved topic name
                else if (msgTypeSplit[0] == "sensor_msgs" && msgTypeSplit[2] == "Image")
                {
                    if (ElementInVector(i.first, this->zedRGBTopicNameList))
                    {
                        std::string subNodeName = i.first.substr(1) + "_node";
                        replace_all(subNodeName, "/", "_");
                        i.second.node = std::make_shared<RGBMatSubNode>(subNodeName, outputFilename, this->globalImgQue_, i.first);
                    }
                    else if (ElementInVector(i.first, this->zedDepthTopicNameList))
                    {
                        std::string subNodeName = i.first.substr(1) + "_node";
                        replace_all(subNodeName, "/", "_");
                        i.second.node = std::make_shared<DepthMatSubNode>(subNodeName, outputFilename, this->globalImgQue_, i.first);
                    }
                    else
                        continue;

                    this->execMap_[i.first] = new rclcpp::executors::SingleThreadedExecutor();
                    this->execMap_[i.first]->add_node(i.second.node);
                    this->subThMap_[i.first] = std::thread(SpinTopicRecordNodeExecutor, this->execMap_[i.first], i.second.node, i.first);
                    
                    printf("Subscribed [%s][%s]\n", i.first.c_str(), msgTypeSplit[2].c_str());
                    i.second.occupyF = true;
                }
            }
            if (this->stopMonitorF_)
            {
                topicContainerLocker.unlock();
                return;
            }
        }
        topicContainerLocker.unlock();


        // Search node names
        if (!this->nodeDetectF_)
            return;
        
        auto nds = this->get_node_names();
        std::unique_lock<std::mutex> nodeContainerLocker(this->nodeContainerPackLock_, std::defer_lock);
        nodeContainerLocker.lock();
        for (auto &i : this->nodeContainerPack_)
            i.second.occupyF = false;

        for (auto &i : nds)
        {
            if (this->nodeContainerPack_.find(i) == this->nodeContainerPack_.end())
            {
                this->nodeContainerPack_[i] = this->nodeTopicMsgTable_.getNodeProperties(i);
                printf("New Node [%s]\n", i.c_str());
                if (this->nodeContainerPack_[i].isGoodF)
                    for (auto &j : this->nodeContainerPack_[i].topicPatternVec)
                        printf("\t[%s][%s]\n", j.first.c_str(), j.second.c_str());
            }
            if (this->nodeContainerPack_[i].isGoodF)
                this->nodeContainerPack_[i].occupyF = true;
        }

        std::vector<std::string> deleteNodeNameVec;
        for (auto &i : this->nodeContainerPack_)
            if (!i.second.occupyF && i.second.isGoodF)
                deleteNodeNameVec.push_back(i.first);
        
        std::vector<std::string> deleteTopicNameVec;
        std::smatch match;
        for (auto &i : deleteNodeNameVec)
        {
            for (auto &j : this->execMap_)
            {
                for (auto &k : this->nodeContainerPack_[i].topicPatternVec)
                    if (std::regex_match(j.first, match, std::regex(k.first)))
                    {
                        printf("Topic [%s][%s] no response. Deleting...\n", j.first.c_str(), k.second.c_str());
                        deleteTopicNameVec.push_back(j.first);
                        j.second->cancel();

                        topicContainerLocker.lock();
                        this->topicContainerPack_.erase(j.first);
                        topicContainerLocker.unlock();
                    }
            }
            this->nodeContainerPack_.erase(i);
            printf("Node [%s] no response. Deleted\n", i.c_str());
        }

        for (auto &i : deleteTopicNameVec)
        {
            this->subThMap_[i].join();
            printf("[%s] joined\n", i.c_str());
            this->subThMap_.erase(i);
            this->execMap_.erase(i);
            printf("[%s] removed\n", i.c_str());
        }

        nodeContainerLocker.unlock();
    }

    void _sampleTimerCallback()
    {
        std::unique_lock<std::mutex> topicContainerLocker(this->topicContainerPackLock_, std::defer_lock);
        std::unique_lock<std::mutex> locker(this->outPackPtrLock_, std::defer_lock);
        
        topicContainerLocker.lock();
        TopicContainerPack topicContainerPackTmp = this->topicContainerPack_;
        topicContainerLocker.unlock();
#ifdef TS_MODE
        double sampleTimestamp = this->getTimestamp().seconds();
#else
        double sampleTimestamp = this->get_clock()->now().seconds();
#endif
        locker.lock();
        for (const auto& i : topicContainerPackTmp)
        {
            if (!i.second.occupyF || i.second.node == nullptr)
                continue;
            if (!i.second.node->isInit())
                continue;
            for (const auto& j : i.second.node->getLatestMsgNodePack())
                (*this->packPtr_)[std::to_string(sampleTimestamp)][i.first][j.first] = j.second;
        }
        locker.unlock();
    }

    void _dumpTimerCallback()
    {
        this->dumpJSON();
        std::string outStr = "Global image queue size: <";
        for (const auto& i : this->globalImgQue_->getSize())
            outStr = outStr + std::to_string(i) + ",";
        outStr.back() = '>';
        printf("%s\n", outStr.c_str());
        this->globalImgQue_->shrink_to_fit();

        outStr = "Global ground queue size: <";
        for (const auto& i : this->globalGndQue_->getSize())
            outStr = outStr + std::to_string(i) + ",";
        outStr.back() = '>';
        printf("%s\n", outStr.c_str());
        this->globalGndQue_->shrink_to_fit();
    }

    void _recordTimerCallback()
    {
        printf("[ScanTopicNode::_recordTimerCallback] Stop record process...\n");
        this->close();
        this->recordTimer_->destroy();
    }

public:
    ScanTopicNode(const std::shared_ptr<vehicle_interfaces::GenericParams>& gParams) : 
#ifdef TS_MODE
        TimeSyncNode(gParams->nodeName, gParams->timesyncService, 3000000, 2), 
#endif
        rclcpp::Node(gParams->nodeName), 
        stopMonitorF_(false), 
        exitF_(false), 
        nodeDetectF_(false), 
        outPackBkF_(false)
    {
        this->declare_parameter<float>("topicScanTime_ms", this->topicScanTime_ms);
        this->declare_parameter<std::string>("subscribeMsgPack", this->subscribeMsgPack);
        this->declare_parameter<std::vector<std::string> >("zedRGBTopicNameList", this->zedRGBTopicNameList);
        this->declare_parameter<std::vector<std::string> >("zedDepthTopicNameList", this->zedDepthTopicNameList);
        this->declare_parameter<std::string>("outputFilename", this->outputFilename);
        this->declare_parameter<double>("samplingStep_ms", this->samplingStep_ms);
        this->declare_parameter<double>("autoSaveTime_s", this->autoSaveTime_s);
        this->declare_parameter<double>("recordTime_s", this->recordTime_s);
        this->declare_parameter<int>("numOfImgSaveTh", this->numOfImgSaveTh);
        this->declare_parameter<int>("numOfGndSaveTh", this->numOfGndSaveTh);
        this->declare_parameter<bool>("enabled_record", this->enabled_record);
        this->_getParams();
        this->paramsCallbackHandler = this->add_on_set_parameters_callback(std::bind(&ScanTopicNode::_paramsCallback, this, std::placeholders::_1));
        
        char buf[128];
        if (this->outputFilename.back() != '/')
            this->outputFilename += '/';
        sprintf(buf, "rm -rf %s && mkdir -p %sjson", this->outputFilename.c_str(), this->outputFilename.c_str());
        const int dir_err = system(buf);

        // Node and topic relation table
        this->nodeDetectF_ = this->nodeTopicMsgTable_.loadTable("NodeTopicTable.json");

        this->globalImgQue_ = new SaveImgQueue(this->numOfImgSaveTh, 100);
        this->globalGndQue_ = new SaveQueue<WriteGroundDetectStruct>(this->numOfGndSaveTh, 100);

        this->monitorTimer_ = new Timer(this->topicScanTime_ms, std::bind(&ScanTopicNode::_monitorTimerCallback, this));
        this->monitorTimer_->start();
        this->sampleTimer_ = new Timer(this->samplingStep_ms, std::bind(&ScanTopicNode::_sampleTimerCallback, this));
        this->dumpTimer_ = new Timer(this->autoSaveTime_s * 1000, std::bind(&ScanTopicNode::_dumpTimerCallback, this));
        if (this->recordTime_s > 0)
            this->recordTimer_ = new Timer(this->recordTime_s * 1000, std::bind(&ScanTopicNode::_recordTimerCallback, this));
        else
            this->recordTimer_ = nullptr;
        this->packPtr_ = &this->pack_;
    }

    void startSampling()
    {
        std::lock_guard<std::mutex>(this->externalControlLock_);
        std::cerr << "[ScanTopicNode][startSampling]" << "\n";
#ifdef TS_MODE
        this->outFileTimestamp_ = this->getTimestamp().seconds();
#else
        this->outFileTimestamp_ = this->get_clock()->now().seconds();
#endif
        this->sampleTimer_->start();
        this->dumpTimer_->start();
        if (this->recordTimer_ != nullptr)
            this->recordTimer_->start();
    }

    void stopSampling()
    {
        std::lock_guard<std::mutex>(this->externalControlLock_);
        std::cerr << "[ScanTopicNode][stopSampling]" << "\n";
        this->sampleTimer_->stop();
        this->dumpTimer_->stop();
        if (this->recordTimer_ != nullptr)
            this->recordTimer_->stop();
        this->dumpJSON();// Rest of files in mem
    }

    void dumpJSON()
    {
        size_t packSz = 0;// Current pack size

        std::unique_lock<std::mutex> locker(this->outPackPtrLock_, std::defer_lock);
        locker.lock();
        // Change this->packPtr_ to switch sampling storage buffer
        if (!this->outPackBkF_)
        {
            this->packPtr_ = &this->packBk_;
            packSz = this->pack_.size();
        }
        else
        {
            this->packPtr_ = &this->pack_;
            packSz = this->packBk_.size();
        }
        locker.unlock();

        if (packSz <= 0)
        {
            this->outPackBkF_ = !this->outPackBkF_;
            return;
        }

        double fileTimestamp = this->outFileTimestamp_;
#ifdef TS_MODE
        this->outFileTimestamp_ = this->getTimestamp().seconds();
#else
        this->outFileTimestamp_ = this->get_clock()->now().seconds();
#endif
        auto st = std::chrono::steady_clock::now();
        std::ofstream outFile(this->outputFilename + "json/" + std::to_string(fileTimestamp) + ".json");
        printf("Dump json from %s buffer...\n", this->outPackBkF_ ? "packBk_" : "pack_");
        if (this->outPackBkF_)
        {
            outFile << DumpPackToJSON(this->packBk_) << std::endl;
            this->packBk_.clear();
        }
        else
        {
            outFile << DumpPackToJSON(this->pack_) << std::endl;
            this->pack_.clear();
        }
        printf("Dump %f.json took %f ms\n", fileTimestamp, (std::chrono::steady_clock::now() - st).count() / 1000000.0);
        this->outPackBkF_ = !this->outPackBkF_;
    }
    
    void close()
    {
        if (this->exitF_)// Already closed
            return;
        this->stopMonitorF_ = true;// Set the flag to true to break monitor loop
        this->monitorTimer_->destroy();// Timer destroys while callback function returned
        printf("monitorTimer_ destroyed\n");
        this->sampleTimer_->destroy();// Stop recording data from topics
        printf("sampleTimer_ destroyed\n");
        this->dumpTimer_->destroy();// Stop saving json files
        printf("dumpTimer_ destroyed\n");

        printf("Dumping rest of files...\n");
        this->dumpJSON();// saving rest of data in memories to json file

        // Stop running subscriber nodes
        printf("Closing subscriber...");
        for (auto &i : this->execMap_)
            i.second->cancel();
        printf("done.\n");

        // Join subscriber threads
        printf("Subscriber threads join...");
        for (auto &i : this->subThMap_)
            i.second.join();
        printf("done.\n");

        delete this->globalImgQue_;// Data in queues will be saved while destructor finished
        delete this->globalGndQue_;// Data in queues will be saved while destructor finished

        this->exitF_ = true;// Set exit flag to true to prevent multiple close function calling
    }

    bool isExit() const { return this->exitF_; }
};


int main(int argc, char** argv)
{
    rclcpp::init(argc, argv);
    auto gParams = std::make_shared<vehicle_interfaces::GenericParams>("dataserver_params_node");
    auto scanTopicNode = std::make_shared<ScanTopicNode>(gParams);
    rclcpp::executors::SingleThreadedExecutor* executor = new rclcpp::executors::SingleThreadedExecutor();
    executor->add_node(scanTopicNode);
    std::thread scanTopicNodeTH(SpinNodeExecutor, executor, "scanTopicNodeTH");

    std::this_thread::sleep_for(2s);
    // scanTopicNode->startSampling();

    while (!scanTopicNode->isExit())
        std::this_thread::yield();
    printf("Shutdown scanTopicNodeTH...");
    executor->cancel();
    scanTopicNodeTH.join();
    scanTopicNode->close();
    printf("done.\n");
    rclcpp::shutdown();
    return EXIT_SUCCESS;
}
