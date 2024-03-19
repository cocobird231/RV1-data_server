#include <chrono>
#include <functional>
#include <memory>

#include <map>
#include <array>
#include <deque>
#include <vector>
#include <string>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include <sstream>
#include <algorithm>
#include <regex>

// JSON File
#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <iomanip>
#include <fstream>
#include <cstdlib>

#include "rclcpp/rclcpp.hpp"
#include "vehicle_interfaces/msg/chassis.hpp"
#include "vehicle_interfaces/msg/distance.hpp"
#include "vehicle_interfaces/msg/environment.hpp"
#include "vehicle_interfaces/msg/gps.hpp"
#include "vehicle_interfaces/msg/ground_detect.hpp"
#include "vehicle_interfaces/msg/id_table.hpp"
#include "vehicle_interfaces/msg/image.hpp"
#include "vehicle_interfaces/msg/imu.hpp"
#include "vehicle_interfaces/msg/millit_brake_motor.hpp"
#include "vehicle_interfaces/msg/millit_power_motor.hpp"
#include "vehicle_interfaces/msg/motor_axle.hpp"
#include "vehicle_interfaces/msg/motor_steering.hpp"
#include "vehicle_interfaces/msg/qos_update.hpp"
#include "vehicle_interfaces/msg/steering_wheel.hpp"
#include "vehicle_interfaces/msg/ups.hpp"
#include "vehicle_interfaces/msg/wheel_state.hpp"

#include "vehicle_interfaces/srv/id_server.hpp"

#include "vehicle_interfaces/utils.h"
#include "vehicle_interfaces/vehicle_interfaces.h"

// Image Process
#include <opencv2/opencv.hpp>

using namespace std::chrono_literals;

class placeholder
{
public:
    virtual ~placeholder() {}

    virtual placeholder* clone() const=0;
};


template<typename ValueType>
class holder : public placeholder
{
public:
    holder(const ValueType & value) : held(value) {}

    virtual placeholder* clone() const { return new holder(held); }

    ValueType get() const { return held; }

private:
    ValueType held;
};


class MsgNode
{
private:
    std::atomic<placeholder*> content_;

public:
    MsgNode() : content_(0) {}

    template<typename T>
    MsgNode(const T& value) : content_(new holder<T>(value)) {}

    MsgNode(const MsgNode& node) : content_(node.content_ ? node.content_.load()->clone() : 0) {}

    ~MsgNode() { delete this->content_; }

    template<typename T>
    void setContent(const T& value)
    {
        this->content_ = new holder<T>(value);
    }

    placeholder* getContent() const { return this->content_.load()->clone(); }

    placeholder* getContent_force() const { return this->content_.load(); }
};


struct WriteBBox2DStruct
{
    float o[2];
    uint8_t origin_type;
    float size[2];
    uint16_t label;
    uint16_t id;
};

struct WriteGroundDetectHeaderStruct// Header of GroundDetect file (10 bytes)
{
    uint8_t prefix;// 0x0F (1 byte)
    uint16_t bbox_cnt;// (2 bytes)
    uint16_t bbox_dsize;// sizeof(WriteBBox2DStruct) (2 bytes)
    uint16_t ground_line_size;// Image width (2 bytes)
    uint16_t ground_line_dsize;// sizeof(int16_t) (2 bytes)
    uint8_t suffix;// 0xF0 (1 byte)
    // sizeof(bbox_dsize) * bbox_cnt
    // sizeof(ground_line_dsize) * ground_line_size
};

struct WriteGroundDetectStruct
{
    WriteGroundDetectHeaderStruct header;
    std::vector<WriteBBox2DStruct> bboxVec;
    std::vector<int16_t> groundLine;
};

template<typename T>
using PairQue = std::deque<std::pair<std::string, T>>;

/**
 * @brief Save Queue
 * @details This class is used to save data in a queue. Each thread has its own queue, and the data will be saved in the queue in a round-robin manner.
 * @tparam T Data type to be saved. Avoid using non-copyable data type and pointer.
 * @note The function _saveCbFunc() need to be sepcialized to save custom data type.
 */
template<typename T>
class SaveQueue
{
private:
    std::deque<PairQue<T> > totalPairQue_;// Each thread's PairQue.

    std::vector<std::thread> thVec_;// Store threads.
    std::vector<std::mutex> queMutexVec_;// Each queue's mutex.
    std::vector<std::condition_variable> queCVVec_;// Each queue's condition variable.
    size_t thNum_;// Number of threads.

    std::atomic<size_t> thSelect_;// Thread selection.
    std::mutex thSelectMutex_;// Thread selection mutex.
    std::atomic<bool> availableF_;// Push available flag.
    std::atomic<bool> exitF_;// Exit flag.

private:
    /**
     * @brief Save thread function.
     * @details Each thread will wait for the condition variable to be notified, and then save the data in the queue.
     * @param[in] queID Thread ID.
     */
    void _saveTh(size_t queID)
    {
        printf("[SaveQueue::_saveTh] Thread %ld started.\n", queID);
        std::unique_lock<std::mutex> locker(this->queMutexVec_[queID], std::defer_lock);
        while (!this->exitF_)
        {
            locker.lock();
            printf("[SaveQueue::_saveTh] Thread %ld waiting...\n", queID);
            this->queCVVec_[queID].wait(locker);
            printf("[SaveQueue::_saveTh] Thread %ld working...\n", queID);
            PairQue<T> tmp(std::make_move_iterator(this->totalPairQue_[queID].begin()), 
                            std::make_move_iterator(this->totalPairQue_[queID].end()));
            this->totalPairQue_[queID].clear();
            locker.unlock();
            if (tmp.size() <= 0)
                continue;
            this->_saveCbFunc(tmp);
        }

        // Check if there is any data left
        locker.lock();
        PairQue<T> tmp(std::make_move_iterator(this->totalPairQue_[queID].begin()), 
                        std::make_move_iterator(this->totalPairQue_[queID].end()));
        this->totalPairQue_[queID].clear();
        locker.unlock();
        if (tmp.size() > 0)
            this->_saveCbFunc(tmp);
        printf("[SaveQueue::_saveTh] Thread %ld exited.\n", queID);
    }

    /**
     * @brief Save callback function.
     * @details This function need to be specialized to save data.
     * @param[in] queue Queue containing <fileName, T> pair.
     */
    void _saveCbFunc(PairQue<T>& queue)
    {
        printf("[SaveQueue::_saveCbFunc] Function not override.\n");
    }

    /**
     * @brief Get the save queue ID.
     * @details This function will return the save queue ID in a round-robin manner.
     * @return size_t Save queue ID.
     */
    size_t _getSaveQueID()
    {
        std::lock_guard<std::mutex> locker(this->thSelectMutex_);
        this->thSelect_ = (++this->thSelect_) % this->thNum_;
        return this->thSelect_;
    }

public:
    /**
     * @brief Construct a new Save Queue object
     * @param[in] thNum Number of threads.
     */
    SaveQueue(size_t thNum = 1) : thSelect_(0), availableF_(true), exitF_(false)
    {
        this->thNum_ = thNum;
        this->queMutexVec_ = std::vector<std::mutex>(thNum);
        this->queCVVec_ = std::vector<std::condition_variable>(thNum);
        this->totalPairQue_ = std::deque<PairQue<T>>(thNum);

        for (size_t i = 0; i < thNum; i++)
            this->thVec_.emplace_back(&SaveQueue::_saveTh, this, i);
    }

    ~SaveQueue()
    {
        this->close();
    }

    /**
     * @brief Push data to the save queue.
     * @param[in] fileName File name.
     * @param[in] element Data to be saved.
     * @note If the push is not available, the data will not be saved.
     */
    void push(const std::string& fileName, const T& element)
    {
        if (this->exitF_ || !this->availableF_)
            return;
        auto id = _getSaveQueID();
        printf("[SaveQueue::push] Got id: %ld\n", id);
        std::unique_lock<std::mutex> locker(this->queMutexVec_[id], std::defer_lock);
        locker.lock();
        this->totalPairQue_[id].emplace_back(fileName, element);
        printf("[SaveQueue::push] Pushed to %ld\n", id);
        this->queCVVec_[id].notify_all();
        locker.unlock();
    }

    /**
     * @brief Get the size of the save queue.
     * @return std::vector<size_t> Size of each save queue.
     */
    std::vector<size_t> getSize()
    {
        std::vector<size_t> ret(thNum_, 0);
        for (size_t i = 0; i < this->thNum_; i++)
        {
            std::lock_guard<std::mutex> locker(this->queMutexVec_[i]);
            ret[i] = this->totalPairQue_[i].size();
        }
        return ret;
    }

    void shrink_to_fit()
    {
        if (this->exitF_)
            return;
        for (size_t i = 0; i < this->thNum_; i++)
        {
            std::lock_guard<std::mutex> locker(this->queMutexVec_[i]);
            //this->totalPairQue_[i].shrink_to_fit();
            PairQue<T>(this->totalPairQue_[i]).swap(this->totalPairQue_[i]);
        }
    }

    /**
     * @brief Close the save queue.
     * @details This function will close the save queue and wait for all threads to exit.
     * @note This function will be called automatically when the object is destroyed.
     */
    void close()
    {
        if (this->exitF_)
            return;
        this->exitF_ = true;
        std::this_thread::sleep_for(100ms);

        for (int i = 0; i < this->thNum_; i++)
        {
            this->queCVVec_[i].notify_all();// Notify all threads to exit.
            this->thVec_[i].join();
        }
    }

    /**
     * @brief Enable push.
     * @param[in] flag Enable or disable push.
     * @note If the push is not available, the data will not be saved.
     */
    void enablePush(bool flag)
    {
        this->availableF_ = flag;
    }
};

/**
 * @brief Specialization of SaveQueue for cv::Mat data type.
 * @details This specialization will save cv::Mat data to file.
 * @param[in] queue Queue containing <fileName, cv::Mat> pair.
 */
template<>
void SaveQueue<cv::Mat>::_saveCbFunc(PairQue<cv::Mat>& queue)
{
    printf("[SaveQueue::_saveCbFunc<cv::Mat>] Function called.\n");
    for (const auto& [fileName, data] : queue)
    {
        printf("[SaveQueue::_saveCbFunc<cv::Mat>] Saving %s %dx%d (%d)\n", fileName.c_str(), data.cols, data.rows, data.type());
        cv::imwrite(fileName, data);
    }
}

/**
 * @brief Specialization of SaveQueue for WriteGroundDetectStruct data type.
 * @details This specialization will save WriteGroundDetectStruct data to file.
 * @param[in] queue Queue containing <fileName, WriteGroundDetectStruct> pair.
 */
template<>
void SaveQueue<WriteGroundDetectStruct>::_saveCbFunc(PairQue<WriteGroundDetectStruct>& queue)
{
    printf("[SaveQueue::_saveCbFunc<WriteGroundDetectStruct>] Function called.\n");
    for (const auto& [fileName, data] : queue)
    {
        // Create File
        FILE *fp = fopen(fileName.c_str(), "wb");
        if (fp == NULL)
        {
            printf("failed to open the file: %s\n", fileName.c_str());
            return;
        }

        // Write WriteGroundDetectStruct to file implementation
        fwrite(&data.header, sizeof(WriteGroundDetectHeaderStruct), 1, fp);
        fwrite(data.bboxVec.data(), sizeof(WriteBBox2DStruct), data.bboxVec.size(), fp);
        fwrite(data.groundLine.data(), sizeof(int16_t), data.groundLine.size(), fp);
        fclose(fp);
    }
}



/**
 * ================================================================================
 *                  Base Topic Subscription Node Definitions
 * ================================================================================
 */

/**
 * @brief Topic Record Node Property
 * @details This structure is used to store the properties of the topic record node.
 */
struct TopicRecordNodeProp
{
    std::string nodeName;// Node name. Necessary for ROS2 node creation.
    std::string topicName;// Topic name. Necessary for ROS2 subscription creation.
    std::string qosServiceName;// QoS service name. Set to "" if not needed.
    std::string qosDirPath;// QoS directory path. Necessary if QoS service name is set.
    std::string outputDir;// Create output directory. Set to "" if not needed.
    bool defaultCallback;// Default enable callback.
};

/**
 * @brief Base Topic Record Node
 * @details This class is used as a base class for topic record node.
 * @note This class is used to record the header and message content of the subscribed topic.
 * @note This class is inherited from vehicle_interfaces::PseudoTimeSyncNode and vehicle_interfaces::QoSUpdateNode.
 */
class BaseTopicRecordNode : public vehicle_interfaces::PseudoTimeSyncNode, public vehicle_interfaces::QoSUpdateNode
{
private:
    struct HeaderMsgNodes
    {
        MsgNode priority;
        MsgNode device_type;
        MsgNode device_id;
        MsgNode frame_id;
        MsgNode stamp_type;// Described timestamp status (msg::Header::STAMPTYPE_XXX)
        MsgNode stamp;// Device timestamp (sec in double type)
        MsgNode stamp_offset;// Device timestamp offset (nsec in int64 type)
        MsgNode ref_publish_time_ms;// Referenced publish time interval

        MsgNode record_stamp_type;// QoS analyzing (msg::Header::STAMPTYPE_XXX)
        MsgNode record_stamp;// QoS analyzing (sec in double type)
        MsgNode record_stamp_offset;// QoS analyzing (nsec in int64 type)
        MsgNode record_frame_id;// QoS analyzing (data recv counter between subscription and sampling)

    } headerNodes_;
    std::map<std::string, MsgNode*> latestMsgNodePack_;
    std::mutex latestMsgNodePackLocker_;

    std::atomic<bool> initF_;
    std::atomic<uint64_t> recvFrameID_;

protected:
    const std::string topicName_;
    const std::string nodeName_;
    const std::string outputDir_;
    std::atomic<bool> enableCb_;

private:
    std::string _checkOutputDir(const std::string& dir)
    {
        if (dir != "")// Create output directory
        {
            if (dir.back() == '/')
                return dir;
            else
                return dir + '/';
        }
        return "";
    }

protected:
    BaseTopicRecordNode(const TopicRecordNodeProp& prop) : 
        vehicle_interfaces::PseudoTimeSyncNode(prop.nodeName), 
        vehicle_interfaces::QoSUpdateNode(prop.nodeName, prop.qosServiceName, prop.qosDirPath), 
        rclcpp::Node(prop.nodeName), 
        nodeName_(prop.nodeName), 
        topicName_(prop.topicName), 
        outputDir_(this->_checkOutputDir(prop.outputDir)), 
        enableCb_(prop.defaultCallback) 
    {
        if (this->outputDir_ != "")// Create output directory
        {
            char buf[256];
            sprintf(buf, "mkdir -p %s%s", this->outputDir_.c_str(), this->nodeName_.c_str());
            const int dir_err = system(buf);
        }

        this->initF_ = false;// Not yet initialized.
        this->recvFrameID_ = 0;
        this->latestMsgNodePack_["priority"] = &this->headerNodes_.priority;
        this->latestMsgNodePack_["device_type"] = &this->headerNodes_.device_type;
        this->latestMsgNodePack_["device_id"] = &this->headerNodes_.device_id;
        this->latestMsgNodePack_["frame_id"] = &this->headerNodes_.frame_id;
        this->latestMsgNodePack_["stamp_type"] = &this->headerNodes_.stamp_type;
        this->latestMsgNodePack_["stamp"] = &this->headerNodes_.stamp;
        this->latestMsgNodePack_["stamp_offset"] = &this->headerNodes_.stamp_offset;
        this->latestMsgNodePack_["ref_publish_time_ms"] = &this->headerNodes_.ref_publish_time_ms;

        this->latestMsgNodePack_["record_stamp_type"] = &this->headerNodes_.record_stamp_type;
        this->latestMsgNodePack_["record_stamp"] = &this->headerNodes_.record_stamp;
        this->latestMsgNodePack_["record_stamp_offset"] = &this->headerNodes_.record_stamp_offset;
        this->latestMsgNodePack_["record_frame_id"] = &this->headerNodes_.record_frame_id;
    }

public:
    void setInitF(bool flag) { this->initF_ = flag; }
    
    bool isInit() const { return this->initF_; }

    void setHeaderNodes(vehicle_interfaces::msg::Header::SharedPtr header)
    {
        std::unique_lock<std::mutex> locker(this->latestMsgNodePackLocker_, std::defer_lock);
        locker.lock();
        this->headerNodes_.priority.setContent(header->priority);
        this->headerNodes_.device_type.setContent(header->device_type);
        this->headerNodes_.device_id.setContent(header->device_id);
        this->headerNodes_.frame_id.setContent(header->frame_id);
        this->headerNodes_.stamp_type.setContent(header->stamp_type);
        double stamp = header->stamp.sec + (double)header->stamp.nanosec / 1000000000.0;
        this->headerNodes_.stamp.setContent(stamp);
        this->headerNodes_.stamp_offset.setContent(header->stamp_offset);
        this->headerNodes_.ref_publish_time_ms.setContent(header->ref_publish_time_ms);

        this->headerNodes_.record_stamp_type.setContent(this->getTimestampType());
        this->headerNodes_.record_stamp.setContent(this->getTimestamp().seconds());
        this->headerNodes_.record_stamp_offset.setContent(static_cast<int64_t>(this->getCorrectDuration().nanoseconds()));
        this->headerNodes_.record_frame_id.setContent(this->recvFrameID_++);
        locker.unlock();
    }

    void addLatestMsgNodePackTag(std::string name, MsgNode* msg)
    {
        std::unique_lock<std::mutex> locker(this->latestMsgNodePackLocker_, std::defer_lock);
        locker.lock();
        this->latestMsgNodePack_[name] = msg;
        locker.unlock();
    }

    virtual std::map<std::string, placeholder*> getLatestMsgNodePack()
    {
        std::map<std::string, placeholder*> ret;
        std::unique_lock<std::mutex> locker(this->latestMsgNodePackLocker_, std::defer_lock);
        locker.lock();
        for (const auto& i : this->latestMsgNodePack_)
        {
            if (i.second->getContent_force() != nullptr)
                ret[i.first] = i.second->getContent();
        }
        this->recvFrameID_ = 0;
        locker.unlock();
        return ret;
    }

    std::string getNodeName() const
    {
        return this->nodeName_;
    }

    std::string getTopicName() const
    {
        return this->topicName_;
    }

    void enableTopicCallback(bool flag)
    {
        this->enableCb_ = flag;
    }
};



template<typename topicT, typename msgnodeT>
class TopicRecordNode : public BaseTopicRecordNode
{
private:
    std::shared_ptr<rclcpp::Subscription<topicT> > sub_;

protected:
    msgnodeT dataNodes_;

private:
    void _topicCbFunc(const std::shared_ptr<topicT> msg)
    {
        if (this->enableCb_)
            this->_setMsgContent(msg);
    }

    void _qosCbFunc(std::map<std::string, rclcpp::QoS*> qmap)
    {
        for (const auto& [k, v] : qmap)
        {
            if (k == this->topicName_ || k == (std::string)this->get_namespace() + "/" + this->topicName_)
            {
                this->sub_.reset();
                this->sub_ = this->create_subscription<topicT>(this->topicName_, 
                    *v, std::bind(&TopicRecordNode::_topicCbFunc, this, std::placeholders::_1));
            }
        }
    }

    /**
     * @brief Set message content to MsgNode and header.
     * @param[in] msg Subscription message.
     * @note This function need to be specialized.
     */
    void _setMsgContent(const std::shared_ptr<topicT>& msg)
    {
        RCLCPP_WARN(this->get_logger(), "[TopicRecordNode::_setMsgContent] Function not specialized.");
    }

    /**
     * @brief Add dataNodes_ message node tag to record package.
     * @note This function need to be specialized.
     */
    void _addMsgNodeTag()
    {
        RCLCPP_WARN(this->get_logger(), "[TopicRecordNode::_addMsgNodeTag] Function not specialized.");
    }

public:
    TopicRecordNode(const TopicRecordNodeProp& prop) : 
        BaseTopicRecordNode(prop), 
        rclcpp::Node(prop.nodeName)
    {
        this->_addMsgNodeTag();// Add message node tag to record package

        {
            this->addQoSCallbackFunc(std::bind(&TopicRecordNode::_qosCbFunc, this, std::placeholders::_1));
            vehicle_interfaces::QoSPair qpair = this->addQoSTracking(prop.topicName);
            this->sub_ = this->create_subscription<topicT>(prop.topicName, 
                *qpair.second, std::bind(&TopicRecordNode::_topicCbFunc, this, std::placeholders::_1));
        }
    }
};



template<typename topicT, typename msgnodeT, typename saveT>
class TopicRecordSaveQueueNode : public BaseTopicRecordNode
{
private:
    std::shared_ptr<rclcpp::Subscription<topicT> > sub_;
    SaveQueue<saveT>* que_;

protected:
    msgnodeT dataNodes_;

private:
    void _topicCbFunc(const std::shared_ptr<topicT> msg)
    {
        if (this->enableCb_)
            this->_setMsgContent(msg);
    }

    void _qosCbFunc(std::map<std::string, rclcpp::QoS*> qmap)
    {
        for (const auto& [k, v] : qmap)
        {
            if (k == this->topicName_ || k == (std::string)this->get_namespace() + "/" + this->topicName_)
            {
                this->sub_.reset();
                this->sub_ = this->create_subscription<topicT>(this->topicName_, 
                    *v, std::bind(&TopicRecordSaveQueueNode::_topicCbFunc, this, std::placeholders::_1));
            }
        }
    }

    /**
     * @brief Set message content to MsgNode and header.
     * @param[in] msg Subscription message.
     * @note This function need to be specialized.
     */
    void _setMsgContent(const std::shared_ptr<topicT>& msg)
    {
        RCLCPP_WARN(this->get_logger(), "[TopicRecordSaveQueueNode::_setMsgContent] Function not specialized.");
    }

    /**
     * @brief Add dataNodes_ message node tag to record package.
     * @note This function need to be specialized.
     */
    void _addMsgNodeTag()
    {
        RCLCPP_WARN(this->get_logger(), "[TopicRecordSaveQueueNode::_addMsgNodeTag] Function not specialized.");
    }

public:
    TopicRecordSaveQueueNode(const TopicRecordNodeProp& prop, SaveQueue<saveT>* queue) : 
        BaseTopicRecordNode(prop), 
        rclcpp::Node(prop.nodeName)
    {
        this->que_ = queue;

        this->_addMsgNodeTag();// Add message node tag to record package

        {
            this->addQoSCallbackFunc(std::bind(&TopicRecordSaveQueueNode::_qosCbFunc, this, std::placeholders::_1));
            vehicle_interfaces::QoSPair qpair = this->addQoSTracking(prop.topicName);
            this->sub_ = this->create_subscription<topicT>(prop.topicName, 
                *qpair.second, std::bind(&TopicRecordSaveQueueNode::_topicCbFunc, this, std::placeholders::_1));
        }
    }
};



/**
 * ================================================================================
 *                          Topic Subscription Node Definitions
 * ================================================================================
 */

/**
 * Message Type: Distance
 */
struct DistanceMsgNodes
{
    MsgNode unit_type;
    MsgNode min;
    MsgNode max;
    MsgNode distance;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::Distance, DistanceMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::Distance>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.unit_type.setContent(msg->unit_type);
    this->dataNodes_.min.setContent(msg->min);
    this->dataNodes_.max.setContent(msg->max);
    this->dataNodes_.distance.setContent(msg->distance);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::Distance, DistanceMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("unit_type", &this->dataNodes_.unit_type);
    addLatestMsgNodePackTag("min", &this->dataNodes_.min);
    addLatestMsgNodePackTag("max", &this->dataNodes_.max);
    addLatestMsgNodePackTag("distance", &this->dataNodes_.distance);
}



/**
 * Message Type: Environment
 */
struct EnvironmentMsgNodes
{
    MsgNode unit_type;
    MsgNode temperature;
    MsgNode relative_humidity;
    MsgNode pressure;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::Environment, EnvironmentMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::Environment>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.unit_type.setContent(msg->unit_type);
    this->dataNodes_.temperature.setContent(msg->temperature);
    this->dataNodes_.relative_humidity.setContent(msg->relative_humidity);
    this->dataNodes_.pressure.setContent(msg->pressure);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::Environment, EnvironmentMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("unit_type", &this->dataNodes_.unit_type);
    addLatestMsgNodePackTag("temperature", &this->dataNodes_.temperature);
    addLatestMsgNodePackTag("relative_humidity", &this->dataNodes_.relative_humidity);
    addLatestMsgNodePackTag("pressure", &this->dataNodes_.pressure);
}



/**
 * Message Type: GPS
 */
struct GPSMsgNodes
{
    MsgNode unit_type;
    MsgNode gps_status;
    MsgNode latitude;
    MsgNode longitude;
    MsgNode altitude;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::GPS, GPSMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::GPS>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.unit_type.setContent(msg->unit_type);
    this->dataNodes_.gps_status.setContent(msg->gps_status);
    this->dataNodes_.latitude.setContent(msg->latitude);
    this->dataNodes_.longitude.setContent(msg->longitude);
    this->dataNodes_.altitude.setContent(msg->altitude);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::GPS, GPSMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("unit_type", &this->dataNodes_.unit_type);
    addLatestMsgNodePackTag("gps_status", &this->dataNodes_.gps_status);
    addLatestMsgNodePackTag("latitude", &this->dataNodes_.latitude);
    addLatestMsgNodePackTag("longitude", &this->dataNodes_.longitude);
    addLatestMsgNodePackTag("altitude", &this->dataNodes_.altitude);
}



/**
 * Message Type: GroundDetect
 */
struct GroundDetectMsgNodes
{
    MsgNode rgb_topic_name;
    MsgNode rgb_frame_id;
    MsgNode depth_topic_name;
    MsgNode depth_frame_id;
    MsgNode width;
    MsgNode height;
    MsgNode filename;
};

template<>
void TopicRecordSaveQueueNode<
    vehicle_interfaces::msg::GroundDetect, 
    GroundDetectMsgNodes, 
    WriteGroundDetectStruct
>
::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::GroundDetect>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.rgb_topic_name.setContent(msg->rgb_topic_name);
    this->dataNodes_.rgb_frame_id.setContent(msg->rgb_frame_id);
    this->dataNodes_.depth_topic_name.setContent(msg->depth_topic_name);
    this->dataNodes_.depth_frame_id.setContent(msg->depth_frame_id);
    this->dataNodes_.width.setContent(msg->width);
    this->dataNodes_.height.setContent(msg->height);

    double timestamp = msg->header.stamp.sec + (double)msg->header.stamp.nanosec / 1000000000.0;
    std::string filename = "";
    filename = this->nodeName_ + "/" + std::to_string(timestamp) + ".bin";
    this->dataNodes_.filename.setContent(filename);

    WriteGroundDetectStruct st;

    WriteGroundDetectHeaderStruct header;
    header.prefix = 0x0F;
    header.bbox_cnt = msg->roi.size();
    header.bbox_dsize = sizeof(WriteBBox2DStruct);
    header.ground_line_size = msg->width;
    header.ground_line_dsize = sizeof(int16_t);
    header.suffix = 0xF0;

    std::vector<WriteBBox2DStruct> bboxStructVec;
    for (auto& i : msg->roi)
    {
        WriteBBox2DStruct bbox;
        bbox.o[0] = i.o.x;
        bbox.o[1] = i.o.y;
        bbox.origin_type = i.origin_type;
        bbox.size[0] = i.size.width;
        bbox.size[1] = i.size.height;
        bbox.label = i.label;
        bbox.id = i.id;

        bboxStructVec.push_back(bbox);
    }

    st.header = header;
    st.bboxVec = bboxStructVec;
    st.groundLine = {msg->ground_line.begin(), msg->ground_line.end()};// TODO: To be improved

    this->que_->push(this->outputDir_ + filename, st);
}

template<>
void TopicRecordSaveQueueNode<
    vehicle_interfaces::msg::GroundDetect, 
    GroundDetectMsgNodes, 
    WriteGroundDetectStruct
>
::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("rgb_topic_name", &this->dataNodes_.rgb_topic_name);
    addLatestMsgNodePackTag("rgb_frame_id", &this->dataNodes_.rgb_frame_id);
    addLatestMsgNodePackTag("depth_topic_name", &this->dataNodes_.depth_topic_name);
    addLatestMsgNodePackTag("depth_frame_id", &this->dataNodes_.depth_frame_id);
    addLatestMsgNodePackTag("width", &this->dataNodes_.width);
    addLatestMsgNodePackTag("height", &this->dataNodes_.height);
    addLatestMsgNodePackTag("filename", &this->dataNodes_.filename);
}



/**
 * Message Type: IDTable
 */
struct IDTableMsgNodes
{
    MsgNode idtable;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::IDTable, IDTableMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::IDTable>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.idtable.setContent(msg->idtable);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::IDTable, IDTableMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("idtable", &this->dataNodes_.idtable);
}



/**
 * Message Type: Image
 */
struct ImageMsgNodes
{
        MsgNode depth_valid_min;
        MsgNode depth_valid_max;
        MsgNode width;
        MsgNode height;
        MsgNode filename;
};

template<>
void TopicRecordSaveQueueNode<
    vehicle_interfaces::msg::Image, 
    ImageMsgNodes, 
    cv::Mat
>
::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::Image>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.depth_valid_min.setContent(msg->depth_valid_min);
    this->dataNodes_.depth_valid_max.setContent(msg->depth_valid_max);
    this->dataNodes_.width.setContent(msg->width);
    this->dataNodes_.height.setContent(msg->height);
    double timestamp = msg->header.stamp.sec + (double)msg->header.stamp.nanosec / 1000000000.0;
    std::string filename = "";
    if (msg->format_type == vehicle_interfaces::msg::Image::FORMAT_JPEG)
        filename = this->nodeName_ + "/" + std::to_string(timestamp) + ".jpg";
    else if (msg->format_type == vehicle_interfaces::msg::Image::FORMAT_RAW && msg->cvmat_type == CV_32FC1)
        filename = this->nodeName_ + "/" + std::to_string(timestamp) + ".tiff";
    this->dataNodes_.filename.setContent(filename);

    try
    {
        cv::Mat tmp;
        if (msg->format_type == vehicle_interfaces::msg::Image::FORMAT_JPEG)
            tmp = cv::imdecode(msg->data, 1);
        else if (msg->format_type == vehicle_interfaces::msg::Image::FORMAT_RAW && msg->cvmat_type == CV_32FC1)
        {
            float *depths = reinterpret_cast<float *>(&msg->data[0]);
            tmp = cv::Mat(msg->height, msg->width, CV_32FC1, depths);
        }
        else
            throw -1;
        this->que_->push(this->outputDir_ + filename, tmp.clone());
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    catch(...)
    {
        std::cerr << this->nodeName_ << " Unknown Exception.\n";
    }
}

template<>
void TopicRecordSaveQueueNode<
    vehicle_interfaces::msg::Image, 
    ImageMsgNodes, 
    cv::Mat
>
::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("depth_valid_min", &this->dataNodes_.depth_valid_min);
    addLatestMsgNodePackTag("depth_valid_max", &this->dataNodes_.depth_valid_max);
    addLatestMsgNodePackTag("width", &this->dataNodes_.width);
    addLatestMsgNodePackTag("height", &this->dataNodes_.height);
    addLatestMsgNodePackTag("filename", &this->dataNodes_.filename);
}



/**
 * Message Type: IMU
 */
struct IMUMsgNodes
{
    MsgNode unit_type;
    MsgNode orientation;
    MsgNode angular_velocity;
    MsgNode linear_acceleration;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::IMU, IMUMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::IMU>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.unit_type.setContent(msg->unit_type);
    this->dataNodes_.orientation.setContent(msg->orientation);
    this->dataNodes_.angular_velocity.setContent(msg->angular_velocity);
    this->dataNodes_.linear_acceleration.setContent(msg->linear_acceleration);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::IMU, IMUMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("unit_type", &this->dataNodes_.unit_type);
    addLatestMsgNodePackTag("orientation", &this->dataNodes_.orientation);
    addLatestMsgNodePackTag("angular_velocity", &this->dataNodes_.angular_velocity);
    addLatestMsgNodePackTag("linear_acceleration", &this->dataNodes_.linear_acceleration);
}



/**
 * Message Type: MillitBrakeMotor
 */
struct MillitBrakeMotorMsgNodes
{
    MsgNode travel_min;
    MsgNode travel_max;
    MsgNode travel;
    MsgNode brake_percentage;
    MsgNode external_control;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::MillitBrakeMotor, MillitBrakeMotorMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::MillitBrakeMotor>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.travel_min.setContent(msg->travel_min);
    this->dataNodes_.travel_max.setContent(msg->travel_max);
    this->dataNodes_.travel.setContent(msg->travel);
    this->dataNodes_.brake_percentage.setContent(msg->brake_percentage);
    this->dataNodes_.external_control.setContent(msg->external_control);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::MillitBrakeMotor, MillitBrakeMotorMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("travel_min", &this->dataNodes_.travel_min);
    addLatestMsgNodePackTag("travel_max", &this->dataNodes_.travel_max);
    addLatestMsgNodePackTag("travel", &this->dataNodes_.travel);
    addLatestMsgNodePackTag("brake_percentage", &this->dataNodes_.brake_percentage);
    addLatestMsgNodePackTag("external_control", &this->dataNodes_.external_control);
}



/**
 * Message Type: MillitPowerMotor
 */
struct MillitPowerMotorMsgNodes
{
    MsgNode motor_mode;
    MsgNode rpm;
    MsgNode torque;
    MsgNode percentage;
    MsgNode voltage;
    MsgNode current;
    MsgNode temperature;
    MsgNode parking;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::MillitPowerMotor, MillitPowerMotorMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::MillitPowerMotor>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.motor_mode.setContent(msg->motor_mode);
    this->dataNodes_.rpm.setContent(msg->rpm);
    this->dataNodes_.torque.setContent(msg->torque);
    this->dataNodes_.percentage.setContent(msg->percentage);
    this->dataNodes_.voltage.setContent(msg->voltage);
    this->dataNodes_.current.setContent(msg->current);
    this->dataNodes_.temperature.setContent(msg->temperature);
    this->dataNodes_.parking.setContent(msg->parking);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::MillitPowerMotor, MillitPowerMotorMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("motor_mode", &this->dataNodes_.motor_mode);
    addLatestMsgNodePackTag("rpm", &this->dataNodes_.rpm);
    addLatestMsgNodePackTag("torque", &this->dataNodes_.torque);
    addLatestMsgNodePackTag("percentage", &this->dataNodes_.percentage);
    addLatestMsgNodePackTag("voltage", &this->dataNodes_.voltage);
    addLatestMsgNodePackTag("current", &this->dataNodes_.current);
    addLatestMsgNodePackTag("temperature", &this->dataNodes_.temperature);
    addLatestMsgNodePackTag("parking", &this->dataNodes_.parking);
}



/**
 * Message Type: MotorAxle
 */
struct MotorAxleMsgNodes
{
    MsgNode dir;
    MsgNode pwm;
    MsgNode parking;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::MotorAxle, MotorAxleMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::MotorAxle>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.dir.setContent(msg->dir);
    this->dataNodes_.pwm.setContent(msg->pwm);
    this->dataNodes_.parking.setContent(msg->parking);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::MotorAxle, MotorAxleMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("dir", &this->dataNodes_.dir);
    addLatestMsgNodePackTag("pwm", &this->dataNodes_.pwm);
    addLatestMsgNodePackTag("parking", &this->dataNodes_.parking);
}



/**
 * Message Type: MotorSteering
 */
struct MotorSteeringMsgNodes
{
    MsgNode unit_type;
    MsgNode min;
    MsgNode max;
    MsgNode center;
    MsgNode value;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::MotorSteering, MotorSteeringMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::MotorSteering>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.unit_type.setContent(msg->unit_type);
    this->dataNodes_.min.setContent(msg->min);
    this->dataNodes_.max.setContent(msg->max);
    this->dataNodes_.center.setContent(msg->center);
    this->dataNodes_.value.setContent(msg->value);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::MotorSteering, MotorSteeringMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("unit_type", &this->dataNodes_.unit_type);
    addLatestMsgNodePackTag("min", &this->dataNodes_.min);
    addLatestMsgNodePackTag("max", &this->dataNodes_.max);
    addLatestMsgNodePackTag("center", &this->dataNodes_.center);
    addLatestMsgNodePackTag("value", &this->dataNodes_.value);
}



/**
 * Message Type: UPS
 */
struct UPSMsgNodes
{
    MsgNode volt_in;
    MsgNode amp_in;
    MsgNode volt_out;
    MsgNode amp_out;
    MsgNode temperature;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::UPS, UPSMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::UPS>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.volt_in.setContent(msg->volt_in);
    this->dataNodes_.amp_in.setContent(msg->amp_in);
    this->dataNodes_.volt_out.setContent(msg->volt_out);
    this->dataNodes_.amp_out.setContent(msg->amp_out);
    this->dataNodes_.temperature.setContent(msg->temperature);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::UPS, UPSMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("volt_in", &this->dataNodes_.volt_in);
    addLatestMsgNodePackTag("amp_in", &this->dataNodes_.amp_in);
    addLatestMsgNodePackTag("volt_out", &this->dataNodes_.volt_out);
    addLatestMsgNodePackTag("amp_out", &this->dataNodes_.amp_out);
    addLatestMsgNodePackTag("temperature", &this->dataNodes_.temperature);
}



/**
 * Message Type: WheelState
 */
struct WheelStateMsgNodes
{
    MsgNode gear;
    MsgNode steering;
    MsgNode pedal_throttle;
    MsgNode pedal_brake;
    MsgNode pedal_clutch;
    MsgNode button;
    MsgNode func;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::WheelState, WheelStateMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::WheelState>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.gear.setContent(msg->gear);
    this->dataNodes_.steering.setContent(msg->steering);
    this->dataNodes_.pedal_throttle.setContent(msg->pedal_throttle);
    this->dataNodes_.pedal_brake.setContent(msg->pedal_brake);
    this->dataNodes_.pedal_clutch.setContent(msg->pedal_clutch);
    this->dataNodes_.button.setContent(msg->button);
    this->dataNodes_.func.setContent(msg->func);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::WheelState, WheelStateMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("gear", &this->dataNodes_.gear);
    addLatestMsgNodePackTag("steering", &this->dataNodes_.steering);
    addLatestMsgNodePackTag("pedal_throttle", &this->dataNodes_.pedal_throttle);
    addLatestMsgNodePackTag("pedal_brake", &this->dataNodes_.pedal_brake);
    addLatestMsgNodePackTag("pedal_clutch", &this->dataNodes_.pedal_clutch);
    addLatestMsgNodePackTag("button", &this->dataNodes_.button);
    addLatestMsgNodePackTag("func", &this->dataNodes_.func);
}



/**
 * Message Type: Chassis
 */
struct ChassisMsgNodes
{
    MsgNode unit_type;
    MsgNode drive_motor;
    MsgNode steering_motor;
    MsgNode brake_motor;
    MsgNode parking_signal;
    MsgNode controller_frame_id;
    MsgNode controller_interrupt;
    MsgNode controller_name;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::Chassis, ChassisMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::Chassis>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.unit_type.setContent(msg->unit_type);
    this->dataNodes_.drive_motor.setContent(msg->drive_motor);
    this->dataNodes_.steering_motor.setContent(msg->steering_motor);
    this->dataNodes_.brake_motor.setContent(msg->brake_motor);
    this->dataNodes_.parking_signal.setContent(msg->parking_signal);
    this->dataNodes_.controller_frame_id.setContent(msg->controller_frame_id);
    this->dataNodes_.controller_interrupt.setContent(msg->controller_interrupt);
    this->dataNodes_.controller_name.setContent(msg->controller_name);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::Chassis, ChassisMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("unit_type", &this->dataNodes_.unit_type);
    addLatestMsgNodePackTag("drive_motor", &this->dataNodes_.drive_motor);
    addLatestMsgNodePackTag("steering_motor", &this->dataNodes_.steering_motor);
    addLatestMsgNodePackTag("brake_motor", &this->dataNodes_.brake_motor);
    addLatestMsgNodePackTag("parking_signal", &this->dataNodes_.parking_signal);
    addLatestMsgNodePackTag("controller_frame_id", &this->dataNodes_.controller_frame_id);
    addLatestMsgNodePackTag("controller_interrupt", &this->dataNodes_.controller_interrupt);
    addLatestMsgNodePackTag("controller_name", &this->dataNodes_.controller_name);
}



/**
 * Message Type: SteeringWheel
 */
struct SteeringWheelMsgNodes
{
    MsgNode gear;
    MsgNode steering;
    MsgNode pedal_throttle;
    MsgNode pedal_brake;
    MsgNode pedal_clutch;
    MsgNode func_0;
    MsgNode func_1;
    MsgNode func_2;
    MsgNode func_3;
    MsgNode controller_frame_id;
    MsgNode controller_interrupt;
    MsgNode controller_name;
};

template<>
void TopicRecordNode<vehicle_interfaces::msg::SteeringWheel, SteeringWheelMsgNodes>::_setMsgContent(const std::shared_ptr<vehicle_interfaces::msg::SteeringWheel>& msg)
{
    this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
    this->dataNodes_.gear.setContent(msg->gear);
    this->dataNodes_.steering.setContent(msg->steering);
    this->dataNodes_.pedal_throttle.setContent(msg->pedal_throttle);
    this->dataNodes_.pedal_brake.setContent(msg->pedal_brake);
    this->dataNodes_.pedal_clutch.setContent(msg->pedal_clutch);
    this->dataNodes_.func_0.setContent(msg->func_0);
    this->dataNodes_.func_1.setContent(msg->func_1);
    this->dataNodes_.func_2.setContent(msg->func_2);
    this->dataNodes_.func_3.setContent(msg->func_3);
    this->dataNodes_.controller_frame_id.setContent(msg->controller_frame_id);
    this->dataNodes_.controller_interrupt.setContent(msg->controller_interrupt);
    this->dataNodes_.controller_name.setContent(msg->controller_name);
}

template<>
void TopicRecordNode<vehicle_interfaces::msg::SteeringWheel, SteeringWheelMsgNodes>::_addMsgNodeTag()
{
    addLatestMsgNodePackTag("gear", &this->dataNodes_.gear);
    addLatestMsgNodePackTag("steering", &this->dataNodes_.steering);
    addLatestMsgNodePackTag("pedal_throttle", &this->dataNodes_.pedal_throttle);
    addLatestMsgNodePackTag("pedal_brake", &this->dataNodes_.pedal_brake);
    addLatestMsgNodePackTag("pedal_clutch", &this->dataNodes_.pedal_clutch);
    addLatestMsgNodePackTag("func_0", &this->dataNodes_.func_0);
    addLatestMsgNodePackTag("func_1", &this->dataNodes_.func_1);
    addLatestMsgNodePackTag("func_2", &this->dataNodes_.func_2);
    addLatestMsgNodePackTag("func_3", &this->dataNodes_.func_3);
    addLatestMsgNodePackTag("controller_frame_id", &this->dataNodes_.controller_frame_id);
    addLatestMsgNodePackTag("controller_interrupt", &this->dataNodes_.controller_interrupt);
    addLatestMsgNodePackTag("controller_name", &this->dataNodes_.controller_name);
}



using DistanceSubNode = TopicRecordNode<vehicle_interfaces::msg::Distance, DistanceMsgNodes>;
using EnvironmentSubNode = TopicRecordNode<vehicle_interfaces::msg::Environment, EnvironmentMsgNodes>;
using GPSSubNode = TopicRecordNode<vehicle_interfaces::msg::GPS, GPSMsgNodes>;
using GroundDetectSubNode = TopicRecordSaveQueueNode<vehicle_interfaces::msg::GroundDetect, GroundDetectMsgNodes, WriteGroundDetectStruct>;
using IDTableSubNode = TopicRecordNode<vehicle_interfaces::msg::IDTable, IDTableMsgNodes>;
using ImageSubNode = TopicRecordSaveQueueNode<vehicle_interfaces::msg::Image, ImageMsgNodes, cv::Mat>;
using IMUSubNode = TopicRecordNode<vehicle_interfaces::msg::IMU, IMUMsgNodes>;
using MillitBrakeMotorSubNode = TopicRecordNode<vehicle_interfaces::msg::MillitBrakeMotor, MillitBrakeMotorMsgNodes>;
using MillitPowerMotorSubNode = TopicRecordNode<vehicle_interfaces::msg::MillitPowerMotor, MillitPowerMotorMsgNodes>;
using MotorAxleSubNode = TopicRecordNode<vehicle_interfaces::msg::MotorAxle, MotorAxleMsgNodes>;
using MotorSteeringSubNode = TopicRecordNode<vehicle_interfaces::msg::MotorSteering, MotorSteeringMsgNodes>;
using UPSSubNode = TopicRecordNode<vehicle_interfaces::msg::UPS, UPSMsgNodes>;
using WheelStateSubNode = TopicRecordNode<vehicle_interfaces::msg::WheelState, WheelStateMsgNodes>;
using ChassisSubNode = TopicRecordNode<vehicle_interfaces::msg::Chassis, ChassisMsgNodes>;
using SteeringWheelSubNode = TopicRecordNode<vehicle_interfaces::msg::SteeringWheel, SteeringWheelMsgNodes>;



/**
 * ================================================================================
 * Topic Subscribtion Node Definitions End
 * ================================================================================
 */

struct TopicContainer
{
    std::string topicName;
    std::string msgType;
    bool occupyF;
    std::shared_ptr<BaseTopicRecordNode> node;
};

typedef std::map<std::string, TopicContainer> TopicContainerPack;
typedef std::pair<std::string, std::string> TopicType;// {"topic_name" : "msg_type"}

struct NodeContainer
{
    std::string nodeName;
    std::string deviceType;
    int vehicleID;
    int zonalID;
    int deviceID;
    bool isGoodF;
    bool occupyF;
    std::vector<TopicType> topicPatternVec;// One sensor node can publish more than one topic.
};

typedef std::map<std::string, NodeContainer> NodeContainerPack;

typedef std::map<std::string, std::map<std::string, std::map<std::string, placeholder*> > > OutPack;


class NodeTopicMsgTable
{
private:
    nlohmann::json json_;
    std::string nodeNamePattern_;
    std::string topicNamePattern_;
    std::string vzPattern_;
    std::string dnumPattern_;

    bool isTableLoad_;

public:
    NodeTopicMsgTable() : vzPattern_("^[/_]?V[0-9]+[/_]Z[0-9]+"), dnumPattern_("[0-9]+(?=_node$)"), isTableLoad_(false) {}

    /** Load the relation between nodes and topics from NodeTopicTable.json
     * NodeTopicTable.json structure:
     * {
     *      "nodeNamePattern" : "[/_]?V[0-9]+_Z[0-9]+[/_]%s_[0-9]+_node", 
     *      "topicNamePattern" : "[/_]?V%d_Z%d[/_]%s_%d", 
     *      "sensor_type" : {
     *          "device_type" : "Message Type", 
     *          "device_type" : "Message Type"
     *      }, 
     *      ...
     * }
     */
    bool loadTable(std::string filePath)
    {
        try
        {
            std::ifstream f(filePath);
            this->json_ = nlohmann::json::parse(f);
            this->nodeNamePattern_ = this->json_["nodeNamePattern"];
            this->topicNamePattern_ = this->json_["topicNamePattern"];
            this->json_.erase("nodeNamePattern");
            this->json_.erase("topicNamePattern");
            this->isTableLoad_ = true;
            // std::cout << this->json_ << std::endl;
            return true;
        }
        catch (...)
        {
            printf("Load node topic relation file error.\n");
            return false;
        }
    }

    NodeContainer getNodeProperties(std::string nodeName)
    {
        
        if (!this->isTableLoad_)
            throw "No Table Loaded";
        NodeContainer ret;
        ret.isGoodF = false;
        ret.nodeName = nodeName;
        for (nlohmann::json::iterator it = this->json_.begin(); it != this->json_.end(); it++)
        {
            char buf[256];
            sprintf(buf, this->nodeNamePattern_.c_str(), it.key().c_str());
            
            std::smatch match;
            if (!std::regex_match(nodeName, match, std::regex(buf)))// No correct node name pattern found
                continue;
            ret.deviceType = it.key();// Record device type

            // Find vehicle number (Vx_Zx) and device number (SOME_SENSOR_xxx_node)
            if (!std::regex_search(nodeName, match, std::regex(this->vzPattern_)))// No correct node name pattern found
                return ret;
            
            std::string tmp = match.str();
            std::vector<int> vdnum;
            while (std::regex_search(tmp, match, std::regex("[0-9]+")))// Get two numbers: vehicle ID and zonal ID (Vx_Zx)
            {
                vdnum.push_back(std::stoi(match.str()));// Store vehicle ID and zonal ID
                tmp = match.suffix();
            }
            if (vdnum.size() != 2)// Numbers of vehicle and zonal not fit (==2)
                return ret;
            
            if (!std::regex_search(nodeName, match, std::regex(this->dnumPattern_)))// Device ID not found
                return ret;
            vdnum.push_back(std::stoi(match.str()));// Store device ID
            
            // Record vehicle and device number
            ret.vehicleID = vdnum[0];
            ret.zonalID = vdnum[1];
            ret.deviceID = vdnum[2];

            // Find desired topics name
            for (nlohmann::json::iterator itt = it.value().begin(); itt != it.value().end(); itt++)
            {
                sprintf(buf, this->topicNamePattern_.c_str(), ret.vehicleID, ret.zonalID, itt.key().c_str(), ret.deviceID);
                ret.topicPatternVec.emplace_back(buf, itt.value());
            }
            if (ret.topicPatternVec.size() < 1)
                return ret;
            
            ret.isGoodF = true;
            return ret;
        }
    }
};



class SafeMat
{
private:
    cv::Mat _mat;
    std::mutex _lock;

public:
    SafeMat(cv::Mat& inputMat)
    {
        std::unique_lock<std::mutex> lock(this->_lock, std::defer_lock);
        lock.lock();
        this->_mat = inputMat;
        lock.unlock();
    }

    void getSafeMat(cv::Mat& outputMat)
    {
        std::unique_lock<std::mutex> lock(this->_lock, std::defer_lock);
        lock.lock();
        outputMat = this->_mat;
        lock.unlock();
    }

    void setSafeMat(cv::Mat& inputMat)
    {
        std::unique_lock<std::mutex> lock(this->_lock, std::defer_lock);
        lock.lock();
        this->_mat = inputMat;
        lock.unlock();
    }
};

class WorkingRate
{
private:
    float rate_;
    int frameCnt_;
    std::chrono::duration<int, std::milli> interval_ms_;
    vehicle_interfaces::Timer* timer_;

    std::mutex locker_;

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

    void _timerCallback()
    {
        int cnt = this->_safeCall(&this->frameCnt_, this->locker_);
        std::chrono::duration<float> toSec = this->interval_ms_;// Casting msec to sec
        this->_safeSave(&this->rate_, (float)cnt / toSec.count(), this->locker_);
        this->_safeSave(&this->frameCnt_, 0, this->locker_);
    }

public:
    WorkingRate(int interval_ms)
    {
        this->rate_ = 0;
        this->frameCnt_ = 0;
        this->interval_ms_ = std::chrono::milliseconds(interval_ms);
        this->timer_ = new vehicle_interfaces::Timer(interval_ms, std::bind(&WorkingRate::_timerCallback, this));
    }

    ~WorkingRate()
    {
        this->timer_->destroy();
        delete this->timer_;
    }

    void addCnt(int num) { this->_safeSave(&this->frameCnt_, this->frameCnt_ + num, this->locker_); }

    void addOneCnt() { this->_safeSave(&this->frameCnt_, this->frameCnt_ + 1, this->locker_); }

    void start() { this->timer_->start(); }

    void stop() { this->timer_->stop(); }

    float getRate() { return this->_safeCall(&this->rate_, this->locker_); }
};


/*
* Functions
*/
void SpinNodeExecutor(rclcpp::executors::SingleThreadedExecutor* exec, std::string threadName)
{
    std::this_thread::sleep_for(1s);
    std::cerr << threadName << " start..." << std::endl;
    exec->spin();
    std::cerr << threadName << " exit." << std::endl;
}

void SpinTopicRecordNodeExecutor(rclcpp::executors::SingleThreadedExecutor* exec, std::shared_ptr<BaseTopicRecordNode> node, std::string threadName)
{
    std::this_thread::sleep_for(1s);
    node->setInitF(true);
    std::cerr << threadName << " start..." << std::endl;
    exec->spin();
    std::cerr << threadName << " exit." << std::endl;
}


std::vector<std::string> split(std::string str, std::string delimiter)
{
    std::vector<std::string> splitStrings;
    int encodingStep = 0;
    for (size_t i = 0; i < str.length(); i++)
    {
        bool isDelimiter = false;
        for (auto& j : delimiter)
            if (str[i] == j)
            {
                isDelimiter = true;
                break;
            }
        if (!isDelimiter)// Is the spliting character
        {
            encodingStep++;
            if (i == str.length() - 1)
                splitStrings.push_back(str.substr(str.length() - encodingStep, encodingStep));
        }
        else// Is delimiter
        {
            if (encodingStep > 0)// Have characters need to split
                splitStrings.push_back(str.substr(i - encodingStep, encodingStep));
            encodingStep = 0;
        }
    }
    return splitStrings;
}

template<typename T>
bool ElementInVector(const T& element, const std::vector<T>& vec)
{
    bool ret = false;
    for (const auto& i : vec)
        if (element == i)
        {
            ret = true;
            break;
        }
    return ret;
}

nlohmann::json DumpPackToJSON(OutPack& pack)
{
    nlohmann::json json;
    for (const auto& pair : pack)
    {
        std::string timestamp = pair.first;
        for (const auto& topic : pair.second)// pair: {Timestamp, Topics}
        {
            std::string topicName = topic.first;
            for (const auto& msg : topic.second)
            {
                std::string msgName = msg.first;
                placeholder* needsHold = msg.second;
                if (holder<uint8_t>* val = dynamic_cast<holder<uint8_t>* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<uint64_t>* val = dynamic_cast<holder<uint64_t>* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<int16_t>* val = dynamic_cast<holder<int16_t>* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<float>* val = dynamic_cast<holder<float>* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<double>* val = dynamic_cast<holder<double>* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<std::string>* val = dynamic_cast<holder<std::string>* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<int64_t>* val = dynamic_cast<holder<int64_t>* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<std::array<float, 3> >* val = dynamic_cast<holder<std::array<float, 3> >* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<std::array<float, 4> >* val = dynamic_cast<holder<std::array<float, 4> >* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<std::vector<std::string> >* val = dynamic_cast<holder<std::vector<std::string> >* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<uint16_t>* val = dynamic_cast<holder<uint16_t>* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<uint32_t>* val = dynamic_cast<holder<uint32_t>* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
                else if (holder<int8_t>* val = dynamic_cast<holder<int8_t>* >(needsHold))
                    json[timestamp][topic.first][msg.first] = val->get();
            }
        }
    }
    return json;
}
