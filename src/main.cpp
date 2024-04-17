#include "header.h"
#include <rcl_interfaces/msg/set_parameters_result.hpp>

class Params : public vehicle_interfaces::GenericParams
{
private:
    // Callback
    rclcpp::Node::OnSetParametersCallbackHandle::SharedPtr _paramsCallbackHandler;
    std::function<vehicle_interfaces::ReasonResult<bool>(const rclcpp::Parameter)> cbFunc_;
    std::atomic<bool> cbFuncSetF_;

public:
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
        RCLCPP_INFO(this->get_logger(), "[Params::_paramsCallback] Callback function called.");
        rcl_interfaces::msg::SetParametersResult res;
        res.successful = true;
        res.reason = "";

        if (!this->cbFuncSetF_)
            return res;

        for (const auto& param : params)
        {
            try
            {
                auto ret = this->cbFunc_(param);
                res.successful = ret.result;
                res.reason = ret.reason;
            }
            catch (...)
            {
                res.successful = false;
                res.reason = "[Params::_paramsCallback] Caught Unknown Exception!";
            }
        }

        return res;
    }

public:
    Params(std::string nodeName) : vehicle_interfaces::GenericParams(nodeName), 
        cbFuncSetF_(false)
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

        this->_paramsCallbackHandler = this->add_on_set_parameters_callback(std::bind(&Params::_paramsCallback, this, std::placeholders::_1));
    }

    void addCallbackFunc(const std::function<vehicle_interfaces::ReasonResult<bool>(const rclcpp::Parameter)>& callback)
    {
        this->cbFunc_ = callback;
        this->cbFuncSetF_ = true;
    }
};



class ScanTopicNode : public vehicle_interfaces::VehicleServiceNode
{
private:
    // gParams
    std::shared_ptr<Params> params_;
    std::mutex paramsLock_;

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
    std::string outputFilename_;

    // Save image queue, pass into subscribe node which contains Image message type
    SaveQueue<cv::Mat>* globalImgQue_;// OLD
    SaveQueue<WriteGroundDetectStruct>* globalGndQue_;

    // Timer definitions
    std::unique_ptr<vehicle_interfaces::LiteTimer> monitorTimer_;// Scan topics on lan.
    std::unique_ptr<vehicle_interfaces::LiteTimer> sampleTimer_;// Grab topic messages at fixed rate.
    std::unique_ptr<vehicle_interfaces::LiteTimer> dumpTimer_;// Dump packPtr_ buffer into file.
    std::unique_ptr<vehicle_interfaces::LiteTimer> recordTimer_;// Total execute time.

    std::mutex changeSamplingMutex_;// Prevent startSampling() and stopSampling() conflicts.
    bool stopMonitorF_;// Stop monitor timer flag
    bool exitF_;// Stop ScanTopicNode

private:
    template <typename T>
    void _safeSave(T* ptr, const T value, std::mutex& lock)
    {
        std::lock_guard<std::mutex> _lock(lock);
        *ptr = value;
    }

    template <typename T>
    T _safeCall(const T* ptr, std::mutex& lock)
    {
        std::lock_guard<std::mutex> _lock(lock);
        return *ptr;
    }

    vehicle_interfaces::ReasonResult<bool> _paramsCbFunc(const rclcpp::Parameter param)
    {
        std::lock_guard<std::mutex> locker(this->paramsLock_);
        if (param.get_name() == "enabled_record")
        {
            this->params_->enabled_record = param.as_bool();
            if (this->params_->enabled_record)
                this->startSampling();
            else
                this->stopSampling();
            RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_paramsCbFunc] %s: %d", param.get_name().c_str(), param.as_bool());
        }
        else if (param.get_name() == "samplingStep_ms")
        {
            this->params_->samplingStep_ms = param.as_double();
            if (!this->sampleTimer_)
                this->sampleTimer_ = vehicle_interfaces::make_unique_timer(this->params_->samplingStep_ms, std::bind(&ScanTopicNode::_sampleTimerCallback, this));
            else
                this->sampleTimer_->setPeriod(this->params_->samplingStep_ms);
            RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_paramsCbFunc] %s: %lf", param.get_name().c_str(), param.as_double());
        }
        else if (param.get_name() == "autoSaveTime_s")
        {
            this->params_->autoSaveTime_s = param.as_double();
            if (this->dumpTimer_ == nullptr)
                this->dumpTimer_ = vehicle_interfaces::make_unique_timer(this->params_->autoSaveTime_s * 1000, std::bind(&ScanTopicNode::_dumpTimerCallback, this));
            else
                this->dumpTimer_->setPeriod(this->params_->autoSaveTime_s * 1000.0);
            RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_paramsCbFunc] %s: %lf", param.get_name().c_str(), param.as_double());
        }
        else if (param.get_name() == "recordTime_s")
        {
            this->params_->recordTime_s = param.as_double();
            if (this->recordTimer_ == nullptr)
                this->recordTimer_ = vehicle_interfaces::make_unique_timer(this->params_->recordTime_s * 1000, std::bind(&ScanTopicNode::_recordTimerCallback, this));
            else
                this->recordTimer_->setPeriod(this->params_->recordTime_s * 1000.0);
            RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_paramsCbFunc] %s: %lf", param.get_name().c_str(), param.as_double());
        }
        return {true, ""};
    }

    void _timeSyncCallback()
    {
        std::lock_guard<std::mutex> locker(this->topicContainerPackLock_);
        for (auto& i : this->topicContainerPack_)
        {
            if (!i.second.occupyF || i.second.node == nullptr)
                continue;
            if (!i.second.node->isInit())
                continue;
            i.second.node->syncTime(this->getCorrectDuration(), this->getTimestampType());
        }
    }

    void _monitorTimerCallback()
    {
        auto params = this->_safeCall(&this->params_, this->paramsLock_);
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
                RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_monitorTimerCallback] New key [%s]: <%s, %d>", i.first.c_str(), container.msgType.c_str(), container.occupyF);
                if (this->stopMonitorF_)
                {
                    topicContainerLocker.unlock();
                    return;
                }
            }
        }
        topicContainerLocker.unlock();

        topicContainerLocker.lock();
        for (auto& [topicName, container] : this->topicContainerPack_)
        {
            if (!container.occupyF)
            {
                auto msgTypeSplit = vehicle_interfaces::split(container.msgType, "/");
                if (msgTypeSplit[0] == params->subscribeMsgPack && msgTypeSplit[1] == "msg")
                {
                    auto splitTopicName = vehicle_interfaces::split(topicName, "/");
                    std::string subNodeName = splitTopicName.back() + "_subnode";
                    vehicle_interfaces::replace_all(subNodeName, "/", "_");

                    TopicRecordNodeProp prop;
                    prop.nodeName = subNodeName;
                    prop.topicName = topicName;
                    prop.qosServiceName = params->qosService;
                    prop.qosDirPath = params->qosDirPath;
                    prop.outputDir = "";
                    prop.defaultCallback = params->enabled_record;

                    if (msgTypeSplit[2] == "Distance")
                        container.node = std::make_shared<DistanceSubNode>(prop);
                    else if (msgTypeSplit[2] == "Environment")
                        container.node = std::make_shared<EnvironmentSubNode>(prop);
                    else if (msgTypeSplit[2] == "GPS")
                        container.node = std::make_shared<GPSSubNode>(prop);
                    else if (msgTypeSplit[2] == "GroundDetect")
                    {
                        prop.outputDir = this->outputFilename_;
                        container.node = std::make_shared<GroundDetectSubNode>(prop, this->globalGndQue_);
                    }
                    else if (msgTypeSplit[2] == "IDTable")
                        container.node = std::make_shared<IDTableSubNode>(prop);
                    else if (msgTypeSplit[2] == "Image")
                    {
                        prop.outputDir = this->outputFilename_;
                        container.node = std::make_shared<ImageSubNode>(prop, this->globalImgQue_);
                    }
                    else if (msgTypeSplit[2] == "IMU")
                        container.node = std::make_shared<IMUSubNode>(prop);
                    else if (msgTypeSplit[2] == "MillitBrakeMotor")
                        container.node = std::make_shared<MillitBrakeMotorSubNode>(prop);
                    else if (msgTypeSplit[2] == "MillitPowerMotor")
                        container.node = std::make_shared<MillitPowerMotorSubNode>(prop);
                    else if (msgTypeSplit[2] == "MotorAxle")
                        container.node = std::make_shared<MotorAxleSubNode>(prop);
                    else if (msgTypeSplit[2] == "MotorSteering")
                        container.node = std::make_shared<MotorSteeringSubNode>(prop);
                    else if (msgTypeSplit[2] == "UPS")
                        container.node = std::make_shared<UPSSubNode>(prop);
                    else if (msgTypeSplit[2] == "WheelState")
                        container.node = std::make_shared<WheelStateSubNode>(prop);
                    else if (msgTypeSplit[2] == "Chassis")
                        container.node = std::make_shared<ChassisSubNode>(prop);
                    else if (msgTypeSplit[2] == "SteeringWheel")
                        container.node = std::make_shared<SteeringWheelSubNode>(prop);
                    else
                        continue;

                    container.node->syncTime(this->getCorrectDuration(), this->getTimestampType());
                    this->execMap_[topicName] = new rclcpp::executors::SingleThreadedExecutor();
                    this->execMap_[topicName]->add_node(container.node);
                    this->subThMap_[topicName] = std::thread(SpinTopicRecordNodeExecutor, this->execMap_[topicName], container.node, topicName);

                    RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_monitorTimerCallback] Subscribed [%s][%s]", topicName.c_str(), msgTypeSplit[2].c_str());
                    container.occupyF = true;
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
                RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_monitorTimerCallback] New Node [%s]", i.c_str());
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
                        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_monitorTimerCallback] Topic [%s][%s] no response. Deleting...", j.first.c_str(), k.second.c_str());
                        deleteTopicNameVec.push_back(j.first);
                        j.second->cancel();

                        topicContainerLocker.lock();
                        this->topicContainerPack_.erase(j.first);
                        topicContainerLocker.unlock();
                    }
            }
            this->nodeContainerPack_.erase(i);
            RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_monitorTimerCallback] Node [%s] no response. Deleted", i.c_str());
        }

        for (auto &i : deleteTopicNameVec)
        {
            this->subThMap_[i].join();
            this->subThMap_.erase(i);
            delete this->execMap_[i];
            this->execMap_.erase(i);
            RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_monitorTimerCallback] [%s] removed.", i.c_str());
        }

        nodeContainerLocker.unlock();
    }

    void _sampleTimerCallback()
    {
        std::lock_guard<std::mutex> topicContainerLocker(this->topicContainerPackLock_);
        std::lock_guard<std::mutex> outPackPtrLocker(this->outPackPtrLock_);

        double sampleTimestamp = this->getTimestamp().seconds();
        for (const auto& i : this->topicContainerPack_)
        {
            if (!i.second.occupyF || i.second.node == nullptr)
                continue;
            if (!i.second.node->isInit())
                continue;
            for (const auto& j : i.second.node->getLatestMsgNodePack())
                (*this->packPtr_)[std::to_string(sampleTimestamp)][i.first][j.first] = j.second;
        }
    }

    void _dumpTimerCallback()
    {
        this->dumpJSON();
        std::string outStr = "Global image queue size: <";
        for (const auto& i : this->globalImgQue_->getSize())
            outStr = outStr + std::to_string(i) + ",";
        outStr.back() = '>';
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_dumpTimerCallback] %s", outStr.c_str());
        this->globalImgQue_->shrink_to_fit();

        outStr = "Global ground queue size: <";
        for (const auto& i : this->globalGndQue_->getSize())
            outStr = outStr + std::to_string(i) + ",";
        outStr.back() = '>';
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_dumpTimerCallback] %s", outStr.c_str());
        this->globalGndQue_->shrink_to_fit();
    }

    void _recordTimerCallback()
    {
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::_recordTimerCallback] Stop record process...");
        this->close();
        this->recordTimer_->destroy();
    }

public:
    ScanTopicNode(const std::shared_ptr<Params>& params) : 
        vehicle_interfaces::VehicleServiceNode(params), 
        rclcpp::Node(params->nodeName), 
        params_(params), 
        nodeDetectF_(false), 
        packPtr_(nullptr), 
        outPackBkF_(false), 
        globalImgQue_(nullptr), 
        globalGndQue_(nullptr), 
        stopMonitorF_(false), 
        exitF_(false)
    {
        this->outputFilename_ = params->outputFilename + "_" + std::to_string(this->getTimestamp().seconds());

        char buf[128];
        if (this->outputFilename_.back() != '/')
            this->outputFilename_ += '/';
        sprintf(buf, "rm -rf %s && mkdir -p %sjson", this->outputFilename_.c_str(), this->outputFilename_.c_str());
        const int dir_err = system(buf);

        // Params callback.
        this->params_->addCallbackFunc(std::bind(&ScanTopicNode::_paramsCbFunc, this, std::placeholders::_1));

        // Time sync callback.
        this->addTimeSyncCallbackFunc(std::bind(&ScanTopicNode::_timeSyncCallback, this));

        // Node and topic relation table.
        this->nodeDetectF_ = this->nodeTopicMsgTable_.loadTable("NodeTopicTable.json");

        // Create SaveQueue.
        this->globalImgQue_ = new SaveQueue<cv::Mat>(params->numOfImgSaveTh);
        this->globalGndQue_ = new SaveQueue<WriteGroundDetectStruct>(params->numOfGndSaveTh);

        if (params->topicScanTime_ms > 0)
        {
            this->monitorTimer_ = vehicle_interfaces::make_unique_timer(params->topicScanTime_ms, std::bind(&ScanTopicNode::_monitorTimerCallback, this));
            this->monitorTimer_->start();
        }

        if (params->samplingStep_ms > 0)
            this->sampleTimer_ = vehicle_interfaces::make_unique_timer(params->samplingStep_ms, std::bind(&ScanTopicNode::_sampleTimerCallback, this));
        if (params->autoSaveTime_s > 0)
            this->dumpTimer_ = vehicle_interfaces::make_unique_timer(params->autoSaveTime_s * 1000, std::bind(&ScanTopicNode::_dumpTimerCallback, this));
        if (params->recordTime_s > 0)
            this->recordTimer_ = vehicle_interfaces::make_unique_timer(params->recordTime_s * 1000, std::bind(&ScanTopicNode::_recordTimerCallback, this));

        // Set package saving pointer to main buffer.
        this->packPtr_ = &this->pack_;

        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode] Constructed");
    }

    void startSampling()
    {
        std::lock_guard<std::mutex> locker(this->changeSamplingMutex_);
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::startSampling] Start sampling...");
        this->outFileTimestamp_ = this->getTimestamp().seconds();
        if (this->sampleTimer_)
            this->sampleTimer_->start();
        if (this->dumpTimer_)
            this->dumpTimer_->start();
        if (this->recordTimer_)
            this->recordTimer_->start();

        this->enableAllTopicCallback(true);
    }

    void stopSampling()
    {
        std::lock_guard<std::mutex> locker(this->changeSamplingMutex_);
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::stopSampling] Stop sampling...");

        this->enableAllTopicCallback(false);

        if (this->sampleTimer_)
            this->sampleTimer_->stop();
        if (this->dumpTimer_)
            this->dumpTimer_->stop();
        if (this->recordTimer_)
            this->recordTimer_->stop();
        this->dumpJSON();// Rest of files in mem
    }

    void dumpJSON()
    {
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::dumpJSON] Dumping data to json file...");
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
        this->outFileTimestamp_ = this->getTimestamp().seconds();
        auto st = std::chrono::steady_clock::now();
        std::ofstream outFile(this->outputFilename_ + "json/" + std::to_string(fileTimestamp) + ".json");
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::dumpJSON] Dumping %lf.json from %s buffer...", fileTimestamp, this->outPackBkF_ ? "secondary" : "main");
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
        RCLCPP_INFO(this->get_logger(), "Dump %lf.json took %lf ms\n", fileTimestamp, (std::chrono::steady_clock::now() - st).count() / 1000000.0);
        this->outPackBkF_ = !this->outPackBkF_;
    }

    void enableAllTopicCallback(bool flag)
    {
        std::lock_guard<std::mutex> lock(this->topicContainerPackLock_);
        for (const auto& [topicName, container] : this->topicContainerPack_)
        {
            if (!container.occupyF || container.node == nullptr)
                continue;
            if (!container.node->isInit())
                continue;
            container.node->enableTopicCallback(flag);
        }
    }

    void close()
    {
        if (this->exitF_)// Already closed
            return;
        this->exitF_ = true;

        this->stopMonitorF_ = true;// Set the flag to true to break monitor loop

        this->monitorTimer_.reset(nullptr);
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::close] monitorTimer_ destroyed");

        this->sampleTimer_.reset(nullptr);
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::close] sampleTimer_ destroyed");

        this->dumpTimer_.reset(nullptr);
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::close] dumpTimer_ destroyed");

        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::close] Dumping rest of data to json file...");
        this->dumpJSON();// saving rest of data in memories to json file

        // Stop running subscriber nodes
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::close] Closing subscriber...");
        for (auto &i : this->execMap_)
            i.second->cancel();

        // Join subscriber threads
        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::close] Joining subscriber threads...");
        for (auto &i : this->subThMap_)
            i.second.join();

        for (auto &i : this->execMap_)
            delete i.second;

        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::close] Delete saving queues...");
        if (this->globalImgQue_ != nullptr)
            delete this->globalImgQue_;// Data in queues will be saved while destructor finished
        if (this->globalGndQue_ != nullptr)
            delete this->globalGndQue_;// Data in queues will be saved while destructor finished

        RCLCPP_INFO(this->get_logger(), "[ScanTopicNode::close] Done.");
    }

    bool isExit() const { return this->exitF_; }
};


int main(int argc, char** argv)
{
    rclcpp::init(argc, argv);
    auto params = std::make_shared<Params>("dataserver_params_node");
    auto scanTopicNode = std::make_shared<ScanTopicNode>(params);
    auto paramsExec = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
    paramsExec->add_node(params);
    auto paramsTh = vehicle_interfaces::make_unique_thread(vehicle_interfaces::SpinExecutor, paramsExec, "paramsTh", 1000.0);

    auto scanTopicNodeExec = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
    scanTopicNodeExec->add_node(scanTopicNode);

    if (params->enabled_record)
        scanTopicNode->startSampling();

    printf("[main] Spin scanTopicNodeTH...\n");
    scanTopicNodeExec->spin();// Block.
    scanTopicNodeExec->cancel();

    printf("[main] Shutdown scanTopicNodeTH...\n");
    scanTopicNode->close();

    paramsExec->cancel();
    paramsTh.reset(nullptr);

    printf("[main] done.\n");
    rclcpp::shutdown();
    return EXIT_SUCCESS;
}
