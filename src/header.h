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
#include <atomic>

#include <sstream>
#include <algorithm>
#include <regex>

// JSON File
#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <iomanip>// nlohmann json dependency
#include <fstream>
#include <cstdlib>

#include "rclcpp/rclcpp.hpp"
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
#include "vehicle_interfaces/msg/ups.hpp"
#include "vehicle_interfaces/msg/wheel_state.hpp"

#include "vehicle_interfaces/srv/id_server.hpp"
#include "vehicle_interfaces/vehicle_interfaces.h"

// Image Process
#include <opencv2/opencv.hpp>

//#define NODE_SUBSCRIBE_PRINT

using namespace std::chrono_literals;
using namespace std::placeholders;

enum TopicNodeException { CLOSE_SIGNAL };

void SaveImg(std::string, cv::Mat);


/**
 * This code is referenced from https://rigtorp.se/spinlock/
 */
struct spinlock
{
    std::atomic<bool> lock_ = {0};

    void lock() noexcept
    {
        for (;;)
        {
            // Optimistically assume the lock is free on the first try
            if (!lock_.exchange(true, std::memory_order_acquire))
            {
                return;
            }
            // Wait for lock to be released without generating cache misses
            while (lock_.load(std::memory_order_relaxed))
            {
                // Issue X86 PAUSE or ARM YIELD instruction to reduce contention between
                // hyper-threads
#ifdef __aarch64__
                std::this_thread::yield();
#elif __WIN32
                std::this_thread::yield();
#else
                __builtin_ia32_pause();
#endif
            }
        }
    }

    bool try_lock() noexcept
    {
        // First do a relaxed load to check if lock is free in order to prevent
        // unnecessary cache misses if someone does while(!try_lock())
        return !lock_.load(std::memory_order_relaxed) && 
                !lock_.exchange(true, std::memory_order_acquire);
    }

    void unlock() noexcept
    {
        lock_.store(false, std::memory_order_release);
    }
};



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


typedef std::deque<std::pair<std::string, cv::Mat>> ImgPairQue;
class SaveImgQueue
{
private:
    std::deque<ImgPairQue> totalImgPairQue_;

    std::vector<std::thread> thVec_;
    std::vector<std::timed_mutex> queLockVec_;
    size_t thNum_;

    std::atomic<size_t> thSelect_;
    std::mutex thSelectLock_;

    int interval_ms_;

    std::atomic<bool> exitF_;

private:
    void _saveImgTh(size_t queID)
    {
        std::unique_lock<std::timed_mutex> locker(this->queLockVec_[queID], std::defer_lock);
        ImgPairQue* imgPairQuePtr = &this->totalImgPairQue_[queID];
        bool emptyF = true;
        std::string fileName;
        cv::Mat img;
        while (!(this->exitF_ && emptyF))
        {
            if (locker.try_lock_for(5ms))
            {
                if (imgPairQuePtr->empty())
                {
                    emptyF = true;
                    locker.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(this->interval_ms_));
                    continue;
                }
                const std::pair<std::string, cv::Mat>& item = imgPairQuePtr->front();
                fileName = item.first;
                img = item.second.clone();
                imgPairQuePtr->pop_front();
                emptyF = false;
                locker.unlock();
                cv::imwrite(fileName, img);
            }
            else
                std::this_thread::sleep_for(std::chrono::milliseconds(this->interval_ms_));
        }
    }

    size_t _getSaveImgQueID()
    {
        std::unique_lock<std::mutex> locker(this->thSelectLock_, std::defer_lock);
        size_t ret;
        locker.lock();
        ret = (++this->thSelect_) % this->thNum_;
        this->thSelect_ = ret;
        locker.unlock();
        return ret;
    }

public:
    SaveImgQueue(size_t thNum = 1, int scanInterval_ms = 100) : exitF_(false), thSelect_(0)
    {
        this->thNum_ = thNum;
        this->interval_ms_ = scanInterval_ms;
        
        this->queLockVec_ = std::vector<std::timed_mutex>(thNum);
        this->totalImgPairQue_ = std::deque<ImgPairQue>(thNum);

        for (size_t i = 0; i < thNum; i++)
            this->thVec_.emplace_back(&SaveImgQueue::_saveImgTh, this, i);
    }

    ~SaveImgQueue()
    {
        this->exitF_ = true;
        for (size_t i = 0; i < this->thNum_; i++)
            this->thVec_[i].join();
    }

    void push(const std::string& fileName, cv::Mat& img)
    {
        auto id = _getSaveImgQueID();
        std::unique_lock<std::timed_mutex> locker(this->queLockVec_[id], std::defer_lock);
        locker.lock();
        this->totalImgPairQue_[id].emplace_back(fileName, img);
        locker.unlock();
    }

    std::vector<size_t> getSize()
    {
        std::vector<size_t> ret(thNum_, 0);
        for (size_t i = 0; i < this->thNum_; i++)
        {
            std::unique_lock<std::timed_mutex> locker(this->queLockVec_[i], std::defer_lock);
            locker.lock();
            ret[i] = this->totalImgPairQue_[i].size();
            locker.unlock();
        }
        return ret;
    }

    void shrink_to_fit()
    {
        for (size_t i = 0; i < this->thNum_; i++)
        {
            std::unique_lock<std::timed_mutex> locker(this->queLockVec_[i], std::defer_lock);
            locker.lock();
            //this->totalImgPairQue_[i].shrink_to_fit();
            ImgPairQue(this->totalImgPairQue_[i]).swap(this->totalImgPairQue_[i]);
            locker.unlock();
        }
    }

    void close() { this->exitF_ = true; }
};

/*

*/
template<typename T>
using PairQue = std::deque<std::pair<std::string, T>>;

template<typename T>
class SaveQueue
{
private:
    std::deque<PairQue<T>> totalPairQue_;

    std::vector<std::thread> thVec_;
    std::vector<std::timed_mutex> queLockVec_;
    size_t thNum_;

    std::atomic<size_t> thSelect_;
    std::mutex thSelectLock_;

    int interval_ms_;

    std::atomic<bool> exitF_;

private:
    void _saveTh(size_t queID)// specified
    {
        std::unique_lock<std::timed_mutex> locker(this->queLockVec_[queID], std::defer_lock);
        PairQue<T>* PairQuePtr = &this->totalPairQue_[queID];
        bool emptyF = true;
        while (!(this->exitF_ && emptyF))
        {
            if (locker.try_lock_for(5ms))
            {
                if (PairQuePtr->empty())
                {
                    emptyF = true;
                    locker.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(this->interval_ms_));
                    continue;
                }
                PairQuePtr->pop_front();
                emptyF = false;
                locker.unlock();
                std::cerr << "!!!SaveQueue Unspecified!!!" << "\n";
            }
            else
                std::this_thread::sleep_for(std::chrono::milliseconds(this->interval_ms_));
        }
    }

    size_t _getSaveQueID()
    {
        std::unique_lock<std::mutex> locker(this->thSelectLock_, std::defer_lock);
        size_t ret;
        locker.lock();
        ret = (++this->thSelect_) % this->thNum_;
        this->thSelect_ = ret;
        locker.unlock();
        return ret;
    }

public:
    SaveQueue(size_t thNum = 1, int scanInterval_ms = 100) : exitF_(false), thSelect_(0)
    {
        this->thNum_ = thNum;
        this->interval_ms_ = scanInterval_ms;
        
        this->queLockVec_ = std::vector<std::timed_mutex>(thNum);
        this->totalPairQue_ = std::deque<PairQue<T>>(thNum);

        for (size_t i = 0; i < thNum; i++)
            this->thVec_.emplace_back(&SaveQueue::_saveTh, this, i);
    }

    ~SaveQueue()
    {
        this->exitF_ = true;
        for (size_t i = 0; i < this->thNum_; i++)
            this->thVec_[i].join();
    }

    void push(const std::string& fileName, const T& element)
    {
        auto id = _getSaveQueID();
        std::unique_lock<std::timed_mutex> locker(this->queLockVec_[id], std::defer_lock);
        locker.lock();
        this->totalPairQue_[id].emplace_back(fileName, element);
        locker.unlock();
    }

    std::vector<size_t> getSize()
    {
        std::vector<size_t> ret(thNum_, 0);
        for (size_t i = 0; i < this->thNum_; i++)
        {
            std::unique_lock<std::timed_mutex> locker(this->queLockVec_[i], std::defer_lock);
            locker.lock();
            ret[i] = this->totalPairQue_[i].size();
            locker.unlock();
        }
        return ret;
    }

    void shrink_to_fit()
    {
        for (size_t i = 0; i < this->thNum_; i++)
        {
            std::unique_lock<std::timed_mutex> locker(this->queLockVec_[i], std::defer_lock);
            locker.lock();
            //this->totalPairQue_[i].shrink_to_fit();
            PairQue<T>(this->totalPairQue_[i]).swap(this->totalPairQue_[i]);
            locker.unlock();
        }
    }

    void close() { this->exitF_ = true; }
};

template<>
void SaveQueue<cv::Mat>::_saveTh(size_t queID)// cv::Mat specified TODO: to be validation
{
    std::unique_lock<std::timed_mutex> locker(this->queLockVec_[queID], std::defer_lock);
    PairQue<cv::Mat>* PairQuePtr = &this->totalPairQue_[queID];
    bool emptyF = true;
    std::string fileName;
    cv::Mat element;
    while (!(this->exitF_ && emptyF))
    {
        if (locker.try_lock_for(5ms))
        {
            if (PairQuePtr->empty())
            {
                emptyF = true;
                locker.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(this->interval_ms_));
                continue;
            }
            const std::pair<std::string, cv::Mat>& item = PairQuePtr->front();
            fileName = item.first;
            element = item.second.clone();
            PairQuePtr->pop_front();
            emptyF = false;
            locker.unlock();
            cv::imwrite(fileName, element);
        }
        else
            std::this_thread::sleep_for(std::chrono::milliseconds(this->interval_ms_));
    }
}

template<>
void SaveQueue<WriteGroundDetectStruct>::_saveTh(size_t queID)// WriteGroundDetectStruct specified
{
    std::unique_lock<std::timed_mutex> locker(this->queLockVec_[queID], std::defer_lock);
    PairQue<WriteGroundDetectStruct>* PairQuePtr = &this->totalPairQue_[queID];
    bool emptyF = true;
    std::string fileName;
    WriteGroundDetectStruct element;
    while (!(this->exitF_ && emptyF))
    {
        if (locker.try_lock_for(5ms))
        {
            if (PairQuePtr->empty())
            {
                emptyF = true;
                locker.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(this->interval_ms_));
                continue;
            }
            const std::pair<std::string, WriteGroundDetectStruct>& item = PairQuePtr->front();
            fileName = item.first;
            element = item.second;
            PairQuePtr->pop_front();
            emptyF = false;
            locker.unlock();
            
            // Create File
            FILE *fp = fopen(fileName.c_str(), "wb");
            if (fp == NULL)
            {
                printf("failed to open the file: %s\n", fileName.c_str());
                return;
            }
            
            // Write WriteGroundDetectStruct to file implementation
            fwrite(&element.header, sizeof(WriteGroundDetectHeaderStruct), 1, fp);
            fwrite(element.bboxVec.data(), sizeof(WriteBBox2DStruct), element.bboxVec.size(), fp);
            fwrite(element.groundLine.data(), sizeof(int16_t), element.groundLine.size(), fp);
            fclose(fp);
        }
        else
            std::this_thread::sleep_for(std::chrono::milliseconds(this->interval_ms_));
    }
}


/* ================================================================================
* 							Base Topic Subscription Node Definitions
* ================================================================================
*/

/*
*	Parent class for ROS2 subscription nodes, managing subscription message and formed into message nodes.
*	Recommend children class calling addLatestMsgNodePackTag() at constructor to add self MsgNode pointer to record package.
*	When subscription callback function called, children class can easily pass header message into setHeaderNodes() to formed header nodes, 
*	but the self MsgNode data need to be set manually by calling setContent() provided by MsgNode class.
*/
class TopicRecordNode : public vehicle_interfaces::PseudoTimeSyncNode
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
    std::mutex latestMsgNodePackLocker_;// Change untest (spinlock to mutex)

    std::atomic<bool> initF_;
    std::atomic<uint64_t> recvFrameID_;

public:
    TopicRecordNode(std::string nodeName) : 
        vehicle_interfaces::PseudoTimeSyncNode(nodeName), 
        rclcpp::Node(nodeName)
    {
        this->initF_ = false;
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
};

// TopicRecordNode Add mkdir
class TopicRecordSavingNode : public TopicRecordNode
{
public:
    std::string outputDir_;
    TopicRecordSavingNode(const std::string& nodeName, const std::string& outputDir) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        if (outputDir.back() == '/')
            this->outputDir_ = outputDir;
        else
            this->outputDir_ = outputDir + '/';
        char buf[128];
        sprintf(buf, "mkdir -p %s%s", this->outputDir_.c_str(), nodeName.c_str());
        const int dir_err = system(buf);
    }
};


/* ================================================================================
* 							Topic Subscription Node Definitions
* ================================================================================
*/



/*
* Message Type: Distance
*/
class DistanceSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::Distance>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode unit_type;
        MsgNode min;
        MsgNode max;
        MsgNode distance;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::Distance::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.unit_type.setContent(msg->unit_type);
        this->dataNodes.min.setContent(msg->min);
        this->dataNodes.max.setContent(msg->max);
        this->dataNodes.distance.setContent(msg->distance);
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: %f", msg->distance);
#endif
    }

public:
    DistanceSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("unit_type", &this->dataNodes.unit_type);
        addLatestMsgNodePackTag("min", &this->dataNodes.min);
        addLatestMsgNodePackTag("max", &this->dataNodes.max);
        addLatestMsgNodePackTag("distance", &this->dataNodes.distance);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::Distance>(topicName, 
            10, std::bind(&DistanceSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: Environment
*/
class EnvironmentSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::Environment>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode unit_type;
        MsgNode temperature;
        MsgNode relative_humidity;
        MsgNode pressure;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::Environment::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.unit_type.setContent(msg->unit_type);
        this->dataNodes.temperature.setContent(msg->temperature);
        this->dataNodes.relative_humidity.setContent(msg->relative_humidity);
        this->dataNodes.pressure.setContent(msg->pressure);
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: temp: %f\trh: %f\tpress: %f", msg->temperature, msg->relative_humidity, msg->pressure);
#endif
    }

public:
    EnvironmentSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("unit_type", &this->dataNodes.unit_type);
        addLatestMsgNodePackTag("temperature", &this->dataNodes.temperature);
        addLatestMsgNodePackTag("relative_humidity", &this->dataNodes.relative_humidity);
        addLatestMsgNodePackTag("pressure", &this->dataNodes.pressure);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::Environment>(topicName, 
            10, std::bind(&EnvironmentSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: GPS
*/
class GPSSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::GPS>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode gps_status;
        MsgNode latitude;
        MsgNode longitude;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::GPS::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.gps_status.setContent(msg->gps_status);
        this->dataNodes.latitude.setContent(msg->latitude);
        this->dataNodes.longitude.setContent(msg->longitude);
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: lat: %f\tlon: %f", msg->latitude, msg->longitude);
#endif
    }

public:
    GPSSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("gps_status", &this->dataNodes.gps_status);
        addLatestMsgNodePackTag("latitude", &this->dataNodes.latitude);
        addLatestMsgNodePackTag("longitude", &this->dataNodes.longitude);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::GPS>(topicName, 
            10, std::bind(&GPSSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: GroundDetect
*/
class GroundDetectSubNode : public TopicRecordSavingNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::GroundDetect>::SharedPtr subscription_;
    std::string nodeName_;
    std::mutex callbackLock_;

    /* SaveQueue method */
    SaveQueue<WriteGroundDetectStruct>* saveQue_;

public:
    struct DataMsgNodes
    {
        MsgNode rgb_topic_name;
        MsgNode rgb_frame_id;
        MsgNode depth_topic_name;
        MsgNode depth_frame_id;
        MsgNode width;
        MsgNode height;
        MsgNode filename;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::GroundDetect::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.rgb_topic_name.setContent(msg->rgb_topic_name);
        this->dataNodes.rgb_frame_id.setContent(msg->rgb_frame_id);
        this->dataNodes.depth_topic_name.setContent(msg->depth_topic_name);
        this->dataNodes.depth_frame_id.setContent(msg->depth_frame_id);
        this->dataNodes.width.setContent(msg->width);
        this->dataNodes.height.setContent(msg->height);

        double timestamp = msg->header.stamp.sec + (double)msg->header.stamp.nanosec / 1000000000.0;
        std::string filename = "";
        filename = this->nodeName_ + "/" + std::to_string(timestamp) + ".bin";
        this->dataNodes.filename.setContent(filename);

        std::unique_lock<std::mutex> locker(this->callbackLock_, std::defer_lock);
        locker.lock();
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
            bbox.o[0] = i.o[0];
            bbox.o[1] = i.o[1];
            bbox.origin_type = i.origin_type;
            bbox.size[0] = i.size[0];
            bbox.size[1] = i.size[1];
            bbox.label = i.label;
            bbox.id = i.id;

            bboxStructVec.push_back(bbox);
        }

        st.header = header;
        st.bboxVec = bboxStructVec;
        st.groundLine = {msg->ground_line.begin(), msg->ground_line.end()};// TODO: To be improved

        this->saveQue_->push(this->outputDir_ + filename, st);
        locker.unlock();

#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: [%s]:%ld | [%s]:%ld | (%ld,%ld)", 
            msg->rgb_topic_name, msg->rgb_frame_id, msg->depth_topic_name, msg->depth_frame_id, msg->width, msg->height);
#endif
    }

public:
    GroundDetectSubNode(const std::string& nodeName, const std::string& topicName, const std::string& outputDir, SaveQueue<WriteGroundDetectStruct>* saveQue) : 
        TopicRecordSavingNode(nodeName, outputDir), 
        rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("rgb_topic_name", &this->dataNodes.rgb_topic_name);
        addLatestMsgNodePackTag("rgb_frame_id", &this->dataNodes.rgb_frame_id);
        addLatestMsgNodePackTag("depth_topic_name", &this->dataNodes.depth_topic_name);
        addLatestMsgNodePackTag("depth_frame_id", &this->dataNodes.depth_frame_id);
        addLatestMsgNodePackTag("width", &this->dataNodes.width);
        addLatestMsgNodePackTag("height", &this->dataNodes.height);
        addLatestMsgNodePackTag("filename", &this->dataNodes.filename);

        this->nodeName_ = nodeName;
        this->saveQue_ = saveQue;

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::GroundDetect>(topicName, 
            10, std::bind(&GroundDetectSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: IDTable
*/
class IDTableSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::IDTable>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode idtable;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::IDTable::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.idtable.setContent(msg->idtable);
#ifdef NODE_SUBSCRIBE_PRINT
        std::string outputStr = "";
        for (const auto& i : msg->idtable)
            outputStr = outputStr + "\t" + i;
        RCLCPP_INFO(this->get_logger(), "I heard: table: %s", outputStr.c_str());
#endif
    }

public:
    IDTableSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("idtable", &this->dataNodes.idtable);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::IDTable>(topicName, 
            10, std::bind(&IDTableSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: Image
*/
class ImageSubNode : public TopicRecordSavingNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::Image>::SharedPtr subscription_;
    std::string nodeName_;

    cv::Mat recvMat_;
    std::atomic<bool> initMatF_;
    std::atomic<bool> newMatF_;
    std::mutex recvMatLock_;

    /* SaveImgQueue method */
    SaveImgQueue* saveImgQue_;

public:
    struct DataMsgNodes
    {
        MsgNode width;
        MsgNode height;
        MsgNode filename;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::Image::SharedPtr msg)// JPEG Only
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.width.setContent(msg->width);
        this->dataNodes.height.setContent(msg->height);
        double timestamp = msg->header.stamp.sec + (double)msg->header.stamp.nanosec / 1000000000.0;
        std::string filename = "";
        if (msg->format_type == vehicle_interfaces::msg::Image::FORMAT_JPEG)
            filename = this->nodeName_ + "/" + std::to_string(timestamp) + ".jpg";
        else if (msg->format_type == vehicle_interfaces::msg::Image::FORMAT_RAW && msg->cvmat_type == CV_32FC1)
            filename = this->nodeName_ + "/" + std::to_string(timestamp) + ".tiff";
        this->dataNodes.filename.setContent(filename);
        
        std::unique_lock<std::mutex> lockMat(this->recvMatLock_, std::defer_lock);
        lockMat.lock();
        try
        {
            if (msg->format_type == vehicle_interfaces::msg::Image::FORMAT_JPEG)
                this->recvMat_ = cv::imdecode(msg->data, 1);
            else if (msg->format_type == vehicle_interfaces::msg::Image::FORMAT_RAW && msg->cvmat_type == CV_32FC1)
            {
                float *depths = reinterpret_cast<float *>(&msg->data[0]);
                this->recvMat_ = cv::Mat(msg->height, msg->width, CV_32FC1, depths);
            }
            else
                throw -1;
            this->newMatF_ = true;
            this->recvMat_ = this->recvMat_.clone();
            this->saveImgQue_->push(this->outputDir_ + filename, this->recvMat_);
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        catch(...)
        {
            std::cerr << this->nodeName_ << " Unknown Exception.\n";
        }
        lockMat.unlock();
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: foramt: %d, width: %d, height: %d", 
            msg->format_order_type, msg->width, msg->height);
#endif
    }

public:
    ImageSubNode(const std::string& nodeName, const std::string& topicName, const std::string& outputDir, SaveImgQueue* saveImgQue) : 
        TopicRecordSavingNode(nodeName, outputDir), 
        rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("width", &this->dataNodes.width);
        addLatestMsgNodePackTag("height", &this->dataNodes.height);
        addLatestMsgNodePackTag("filename", &this->dataNodes.filename);

        this->recvMat_ = cv::Mat(1080, 1920, CV_8UC4, cv::Scalar(50));
        this->initMatF_ = true;
        this->nodeName_ = nodeName;
        this->saveImgQue_ = saveImgQue;

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::Image>(topicName, 
            10, std::bind(&ImageSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: IMU
*/
class IMUSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::IMU>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode unit_type;
        MsgNode orientation;
        MsgNode angular_velocity;
        MsgNode linear_acceleration;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::IMU::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.unit_type.setContent(msg->unit_type);
        this->dataNodes.orientation.setContent(msg->orientation);
        this->dataNodes.angular_velocity.setContent(msg->angular_velocity);
        this->dataNodes.linear_acceleration.setContent(msg->linear_acceleration);
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: acc: %f %f %f\tgyro: %f %f %f", 
                    msg->linear_acceleration[0], msg->linear_acceleration[1], msg->linear_acceleration[2], 
                    msg->angular_velocity[0], msg->angular_velocity[1], msg->angular_velocity[2]);
#endif
    }

public:
    IMUSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("unit_type", &this->dataNodes.unit_type);
        addLatestMsgNodePackTag("orientation", &this->dataNodes.orientation);
        addLatestMsgNodePackTag("angular_velocity", &this->dataNodes.angular_velocity);
        addLatestMsgNodePackTag("linear_acceleration", &this->dataNodes.linear_acceleration);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::IMU>(topicName, 
            10, std::bind(&IMUSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: MillitBrakeMotor
*/
class MillitBrakeMotorSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::MillitBrakeMotor>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode travel_min;
        MsgNode travel_max;
        MsgNode travel;
        MsgNode brake_percentage;
        MsgNode external_control;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::MillitBrakeMotor::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.travel_min.setContent(msg->travel_min);
        this->dataNodes.travel_max.setContent(msg->travel_max);
        this->dataNodes.travel.setContent(msg->travel);
        this->dataNodes.brake_percentage.setContent(msg->brake_percentage);
        this->dataNodes.external_control.setContent(msg->external_control);
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: min:%d max:%d travel:%d %f(%) | %d", 
                    msg->travel_min, msg->travel_max, msg->travel, 
                    msg->brake_percentage, msg->external_control);
#endif
    }

public:
    MillitBrakeMotorSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("travel_min", &this->dataNodes.travel_min);
        addLatestMsgNodePackTag("travel_max", &this->dataNodes.travel_max);
        addLatestMsgNodePackTag("travel", &this->dataNodes.travel);
        addLatestMsgNodePackTag("brake_percentage", &this->dataNodes.brake_percentage);
        addLatestMsgNodePackTag("external_control", &this->dataNodes.external_control);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::MillitBrakeMotor>(topicName, 
            10, std::bind(&MillitBrakeMotorSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: MillitPowerMotor
*/
class MillitPowerMotorSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::MillitPowerMotor>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode motor_mode;
        MsgNode rpm;
        MsgNode torque;
        MsgNode percentage;
        MsgNode voltage;
        MsgNode current;
        MsgNode temperature;
        MsgNode parking;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::MillitPowerMotor::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.motor_mode.setContent(msg->motor_mode);
        this->dataNodes.rpm.setContent(msg->rpm);
        this->dataNodes.torque.setContent(msg->torque);
        this->dataNodes.percentage.setContent(msg->percentage);
        this->dataNodes.voltage.setContent(msg->voltage);
        this->dataNodes.current.setContent(msg->current);
        this->dataNodes.temperature.setContent(msg->temperature);
        this->dataNodes.parking.setContent(msg->parking);
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: mode:%d | rpm:%d torq:%f | %f(%) | %fV %fA %fC | park:%d", 
                    msg->motor_mode, msg->rpm, msg->torque, msg->percentage, 
                    msg->voltage, msg->current, msg->temperature, msg->parking);
#endif
    }

public:
    MillitPowerMotorSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("motor_mode", &this->dataNodes.motor_mode);
        addLatestMsgNodePackTag("rpm", &this->dataNodes.rpm);
        addLatestMsgNodePackTag("torque", &this->dataNodes.torque);
        addLatestMsgNodePackTag("percentage", &this->dataNodes.percentage);
        addLatestMsgNodePackTag("voltage", &this->dataNodes.voltage);
        addLatestMsgNodePackTag("current", &this->dataNodes.current);
        addLatestMsgNodePackTag("temperature", &this->dataNodes.temperature);
        addLatestMsgNodePackTag("parking", &this->dataNodes.parking);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::MillitPowerMotor>(topicName, 
            10, std::bind(&MillitPowerMotorSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: MotorAxle
*/
class MotorAxleSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::MotorAxle>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode dir;
        MsgNode pwm;
        MsgNode parking;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::MotorAxle::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.dir.setContent(msg->dir);
        this->dataNodes.pwm.setContent(msg->pwm);
        this->dataNodes.parking.setContent(msg->parking);
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: %d | %f | %d", msg->dir, msg->pwm, msg->parking);
#endif
    }

public:
    MotorAxleSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("dir", &this->dataNodes.dir);
        addLatestMsgNodePackTag("pwm", &this->dataNodes.pwm);
        addLatestMsgNodePackTag("parking", &this->dataNodes.parking);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::MotorAxle>(topicName, 
            10, std::bind(&MotorAxleSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: MotorAxle
*/
class MotorSteeringSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::MotorSteering>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode unit_type;
        MsgNode min;
        MsgNode max;
        MsgNode center;
        MsgNode value;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::MotorSteering::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.unit_type.setContent(msg->unit_type);
        this->dataNodes.min.setContent(msg->min);
        this->dataNodes.max.setContent(msg->max);
        this->dataNodes.center.setContent(msg->center);
        this->dataNodes.value.setContent(msg->value);
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: %d | %f %f %f| %f", msg->unit_type, msg->min, msg->max, msg->center, msg->value);
#endif
    }

public:
    MotorSteeringSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("unit_type", &this->dataNodes.unit_type);
        addLatestMsgNodePackTag("min", &this->dataNodes.min);
        addLatestMsgNodePackTag("max", &this->dataNodes.max);
        addLatestMsgNodePackTag("center", &this->dataNodes.center);
        addLatestMsgNodePackTag("value", &this->dataNodes.value);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::MotorSteering>(topicName, 
            10, std::bind(&MotorSteeringSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: UPS
*/
class UPSSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::UPS>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode volt_in;
        MsgNode amp_in;
        MsgNode volt_out;
        MsgNode amp_out;
        MsgNode temperature;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::UPS::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.volt_in.setContent(msg->volt_in);
        this->dataNodes.amp_in.setContent(msg->amp_in);
        this->dataNodes.volt_out.setContent(msg->volt_out);
        this->dataNodes.amp_out.setContent(msg->amp_out);
        this->dataNodes.temperature.setContent(msg->temperature);
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: %02.2f %02.2f | %02.2f %02.2f | %f", 
            msg->volt_in, msg->amp_in, msg->volt_out, msg->amp_out, msg->temperature);
#endif
    }

public:
    UPSSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("volt_in", &this->dataNodes.volt_in);
        addLatestMsgNodePackTag("amp_in", &this->dataNodes.amp_in);
        addLatestMsgNodePackTag("volt_out", &this->dataNodes.volt_out);
        addLatestMsgNodePackTag("amp_out", &this->dataNodes.amp_out);
        addLatestMsgNodePackTag("temperature", &this->dataNodes.temperature);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::UPS>(topicName, 
            10, std::bind(&UPSSubNode::_topic_callback, this, std::placeholders::_1));
    }
};


/*
* Message Type: WheelState
*/
class WheelStateSubNode : public TopicRecordNode
{
private:
    rclcpp::Subscription<vehicle_interfaces::msg::WheelState>::SharedPtr subscription_;

public:
    struct DataMsgNodes
    {
        MsgNode gear;
        MsgNode steering;
        MsgNode pedal_throttle;
        MsgNode pedal_brake;
        MsgNode pedal_clutch;
        MsgNode button;
        MsgNode func;
    } dataNodes;

private:
    void _topic_callback(const vehicle_interfaces::msg::WheelState::SharedPtr msg)
    {
        this->setHeaderNodes(std::make_shared<vehicle_interfaces::msg::Header>(msg->header));
        this->dataNodes.gear.setContent(msg->gear);
        this->dataNodes.steering.setContent(msg->steering);
        this->dataNodes.pedal_throttle.setContent(msg->pedal_throttle);
        this->dataNodes.pedal_brake.setContent(msg->pedal_brake);
        this->dataNodes.pedal_clutch.setContent(msg->pedal_clutch);
        this->dataNodes.button.setContent(msg->button);
        this->dataNodes.func.setContent(msg->func);
#ifdef NODE_SUBSCRIBE_PRINT
        RCLCPP_INFO(this->get_logger(), "I heard: %03d | %05d %05d %05d %05d | %03d %03d", 
            msg->gear, msg->steering, msg->pedal_throttle, msg->pedal_brake, msg->pedal_clutch, 
            msg->button, msg->func);
#endif
    }

public:
    WheelStateSubNode(const std::string& nodeName, const std::string& topicName) : TopicRecordNode(nodeName), rclcpp::Node(nodeName)
    {
        addLatestMsgNodePackTag("gear", &this->dataNodes.gear);
        addLatestMsgNodePackTag("steering", &this->dataNodes.steering);
        addLatestMsgNodePackTag("pedal_throttle", &this->dataNodes.pedal_throttle);
        addLatestMsgNodePackTag("pedal_brake", &this->dataNodes.pedal_brake);
        addLatestMsgNodePackTag("pedal_clutch", &this->dataNodes.pedal_clutch);
        addLatestMsgNodePackTag("button", &this->dataNodes.button);
        addLatestMsgNodePackTag("func", &this->dataNodes.func);

        this->subscription_ = this->create_subscription<vehicle_interfaces::msg::WheelState>(topicName, 
            10, std::bind(&WheelStateSubNode::_topic_callback, this, std::placeholders::_1));
    }
};



/*
* ZED Image Process
*/

#include "sensor_msgs/msg/image.hpp"
#include "sensor_msgs/msg/camera_info.hpp"
#include "sensor_msgs/image_encodings.hpp"

class BaseMatSubNode : public TopicRecordNode
{
private:
    cv::Mat recvMat_;
    std::atomic<bool> recvMatInitF_;
    std::atomic<bool> newRecvMatF_;
    std::mutex recvMatLock_;

public:
    struct DataMsgNodes
    {
        MsgNode height;
        MsgNode width;
        MsgNode filename;
    } dataNodes;

    std::string nodeName_;
    std::string outputDIR_;

    /* SaveImgQueue method */
    SaveImgQueue* saveImgQue_;

public:
    BaseMatSubNode(const std::string& nodeName, const std::string& outputDIR, SaveImgQueue* saveImgQue) : 
        TopicRecordNode(nodeName), 
        rclcpp::Node(nodeName), 
        recvMatInitF_(false), 
        newRecvMatF_(false)
    {
        addLatestMsgNodePackTag("height", &this->dataNodes.height);
        addLatestMsgNodePackTag("width", &this->dataNodes.width);
        addLatestMsgNodePackTag("filename", &this->dataNodes.filename);
        this->nodeName_ = nodeName;
        // Create <nodeName> directory under <outputDIR> directory
        if (outputDIR.back() == '/')
            this->outputDIR_ = outputDIR;
        else
            this->outputDIR_ = outputDIR + '/';
        char buf[128];
        sprintf(buf, "mkdir -p %s%s", this->outputDIR_.c_str(), this->nodeName_.c_str());
        const int dir_err = system(buf);

        this->saveImgQue_ = saveImgQue;
    }

    void setInitMat(const cv::Mat img)
    {
        std::unique_lock<std::mutex> lockMat(this->recvMatLock_, std::defer_lock);
        
        lockMat.lock();
        this->recvMat_ = img.clone();
        this->recvMatInitF_ = true;
        this->newRecvMatF_ = false;// Test inside
        lockMat.unlock();
    }

    void setRecvMat(const cv::Mat recv)
    {
        std::unique_lock<std::mutex> lockMat(this->recvMatLock_, std::defer_lock);
        lockMat.lock();
        this->recvMat_ = recv;
        this->newRecvMatF_ = true;// Test inside
        lockMat.unlock();
    }

    void setRecvMat_clone(const cv::Mat& recv)
    {
        std::unique_lock<std::mutex> lockMat(this->recvMatLock_, std::defer_lock);
        lockMat.lock();
        this->recvMat_ = recv.clone();
        this->newRecvMatF_ = true;// Test inside
        lockMat.unlock();
    }

    bool getRecvMat_clone(cv::Mat& outputMat)
    {
        std::unique_lock<std::mutex> _lockMat(this->recvMatLock_, std::defer_lock);
        bool ret = false;

        _lockMat.lock();
        if (this->newRecvMatF_ && this->recvMatInitF_)
        {
            outputMat = this->recvMat_.clone();
            this->newRecvMatF_ = false;
            ret = true;
        }
        _lockMat.unlock();
        return ret;
    }
    /*
    std::map<std::string, placeholder*> getLatestMsgNodePack() override
    {
        auto ret = TopicRecordNode::getLatestMsgNodePack();
        if (ret.size() > 0 && this->recvMatInitF_)
        {
            cv::Mat saveMat;
            if (this->getRecvMat_clone(saveMat))
                if (holder<std::string>* val = dynamic_cast<holder<std::string>* >(ret["filename"]))
                    // this->saveQueue_.emplace_back(SaveImg, this->outputDIR_ + val->get(), saveMat);
                    this->saveImgQue_->push(this->outputDIR_ + val->get(), saveMat);
        }
        return ret;
    }*/
};

class RGBMatSubNode : public BaseMatSubNode
{
private:
    rclcpp::Subscription<sensor_msgs::msg::Image>::SharedPtr RGBASub_;

private:
    void _rgbaCallback(const sensor_msgs::msg::Image::SharedPtr msg)
    {
        // Check for correct input image encoding
        if(msg->encoding!=sensor_msgs::image_encodings::BGRA8) {
            RCLCPP_ERROR(get_logger(), "The input topic image requires 'BGRA8' encoding");
            return;
        }

        auto header = std::make_shared<vehicle_interfaces::msg::Header>();
        header->priority = vehicle_interfaces::msg::Header::PRIORITY_SENSOR;
        header->device_type = vehicle_interfaces::msg::Header::DEVTYPE_IMAGE;
        header->device_id = msg->header.frame_id;
        header->stamp_type = vehicle_interfaces::msg::Header::STAMPTYPE_NO_SYNC;
        header->stamp = msg->header.stamp;
        // this->setHeaderNodes(header);
        this->setHeaderNodes(header);
        this->dataNodes.height.setContent(msg->height);
        this->dataNodes.width.setContent(msg->width);
        double timestamp = msg->header.stamp.sec + (double)msg->header.stamp.nanosec / 1000000000.0;
        std::string filename = this->nodeName_ + "/" + std::to_string(timestamp) + ".jpg";
        this->dataNodes.filename.setContent(filename);

        cv::Mat recv(msg->height, msg->width, CV_8UC4, (void*)(&msg->data[0]));
        cv::cvtColor(recv, recv, cv::COLOR_BGRA2BGR);
        this->saveImgQue_->push(this->outputDIR_ + filename, recv);
        this->setRecvMat_clone(recv);
    }

public:
    RGBMatSubNode(const std::string& nodeName, const std::string& outputDIR, SaveImgQueue* saveImgQue, const std::string& topicName) : 
        BaseMatSubNode(nodeName, outputDIR, saveImgQue), 
        rclcpp::Node(nodeName)
    {
        this->setInitMat(cv::Mat(360, 640, CV_8UC4, cv::Scalar(50)));
        // ROS2 QoS
        rclcpp::QoS depth_qos(10);
        depth_qos.keep_last(10);
        depth_qos.best_effort();
        depth_qos.durability_volatile();
        // Create rgba subscriber
        this->RGBASub_ = this->create_subscription<sensor_msgs::msg::Image>(topicName, depth_qos, std::bind(&RGBMatSubNode::_rgbaCallback, this, _1));
    }
};

class DepthMatSubNode : public BaseMatSubNode
{
private:
    rclcpp::Subscription<sensor_msgs::msg::Image>::SharedPtr mDepthSub;

private:
    void _depthCallback(const sensor_msgs::msg::Image::SharedPtr msg)
    {
        // Get a pointer to the depth values casting the data
        // pointer to floating point
        float* depths = (float*)(&msg->data[0]);
        //RCLCPP_INFO(get_logger(), "Centrol: %f %dx%d", depths[msg->width * msg->height / 2 + msg->width / 2], msg->width, msg->height);
        
        auto header = std::make_shared<vehicle_interfaces::msg::Header>();
        header->priority = vehicle_interfaces::msg::Header::PRIORITY_SENSOR;
        header->device_type = vehicle_interfaces::msg::Header::DEVTYPE_IMAGE;
        header->device_id = msg->header.frame_id;
        header->stamp_type = vehicle_interfaces::msg::Header::STAMPTYPE_NO_SYNC;
        header->stamp = msg->header.stamp;
        // this->setHeaderNodes(header);
        this->setHeaderNodes(header);
        this->dataNodes.height.setContent(msg->height);
        this->dataNodes.width.setContent(msg->width);
        double timestamp = msg->header.stamp.sec + (double)msg->header.stamp.nanosec / 1000000000.0;
        std::string filename = this->nodeName_ + "/" + std::to_string(timestamp) + ".tiff";
        this->dataNodes.filename.setContent(filename);

        cv::Mat recv = cv::Mat(msg->height, msg->width, CV_32FC1, depths);
        this->saveImgQue_->push(this->outputDIR_ + filename, recv);
        this->setRecvMat(recv);
    }

public:
    DepthMatSubNode(const std::string& nodeName, const std::string& outputDIR, SaveImgQueue* saveImgQue, const std::string& topicName) : 
        BaseMatSubNode(nodeName, outputDIR, saveImgQue), 
        rclcpp::Node(nodeName)
    {
        this->setInitMat(cv::Mat(360, 640, CV_32FC1, cv::Scalar(50)));
        // ROS2 QoS
        rclcpp::QoS depth_qos(10);
        depth_qos.keep_last(10);
        depth_qos.best_effort();
        depth_qos.durability_volatile();
        // Create depth map subscriber
        this->mDepthSub = create_subscription<sensor_msgs::msg::Image>(topicName, depth_qos, std::bind(&DepthMatSubNode::_depthCallback, this, _1));
    }
};


/* ================================================================================
* 						Topic Subscribtion Node Definitions End
* ================================================================================
*/

struct TopicContainer
{
    std::string topicName;
    std::string msgType;
    bool occupyF;
    std::shared_ptr<TopicRecordNode> node;
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

    /* Load the relation between nodes and topics from NodeTopicTable.json
    * NodeTopicTable.json structure:
    * {
    *		"nodeNamePattern" : "[/_]?V[0-9]+_Z[0-9]+[/_]%s_[0-9]+_node", 
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

void SpinTopicRecordNodeExecutor(rclcpp::executors::SingleThreadedExecutor* exec, std::shared_ptr<TopicRecordNode> node, std::string threadName)
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

/*
* This code is referenced from: https://en.cppreference.com/w/cpp/string/basic_string/replace
*/
std::size_t replace_all(std::string& inout, std::string what, std::string with)
{
    std::size_t count{};
    for (std::string::size_type pos{};
            inout.npos != (pos = inout.find(what.data(), pos, what.length()));
            pos += with.length(), ++count)
    {
        inout.replace(pos, what.length(), with.data(), with.length());
    }
    return count;
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
