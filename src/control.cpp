#include <iostream>
#include <iomanip>
#include <chrono>

#include <rclcpp/rclcpp.hpp>
#include <rcl_interfaces/msg/set_parameters_result.hpp>

#ifndef ROS_DISTRO// 0: eloquent, 1: foxy, 2: humble
#define ROS_DISTRO 1
#endif

#define NODE_NAME "dataservertest_0_node"
#define SERVICE_NAME "/V0/dataserver_params_node/set_parameters_atomically"

using namespace std::chrono_literals;

class ParamsNode : public rclcpp::Node
{
private:
	rclcpp::Node::SharedPtr paramNode_;
	rclcpp::Client<rcl_interfaces::srv::SetParametersAtomically>::SharedPtr paramClient_;

	std::atomic<bool> exitF_;

private:
	void _connToService(rclcpp::ClientBase::SharedPtr client)
    {
        while (!client->wait_for_service(1s))
        {
            if (!rclcpp::ok())
            {
                RCLCPP_ERROR(this->get_logger(), "Interrupted while waiting for the service. Exiting.");
                return;
            }
            RCLCPP_INFO(this->get_logger(), "Service not available, waiting again...");
        }
		RCLCPP_INFO(this->get_logger(), "Service connected.");
    }

	bool _sendRequest(const rcl_interfaces::msg::Parameter& param)
	{
		auto request = std::make_shared<rcl_interfaces::srv::SetParametersAtomically::Request>();
		request->parameters.push_back(param);

		auto result = this->paramClient_->async_send_request(request);
#if ROS_DISTRO == 0
        if (rclcpp::spin_until_future_complete(this->paramNode_, result, 10s) == rclcpp::executor::FutureReturnCode::SUCCESS)
#else
        if (rclcpp::spin_until_future_complete(this->paramNode_, result, 10s) == rclcpp::FutureReturnCode::SUCCESS)
#endif
        {
            auto response = result.get();
            if (response->result.successful)
			{
				printf("[ParamsNode] Parameters set.\n");
			}
			else
				printf("[ParamsNode] Set parameters responsed error: %s\n", response->result.reason.c_str());
			return response->result.successful;
        }
		return false;
	}

public:
    ParamsNode(const std::string& nodeName) : rclcpp::Node(nodeName), exitF_(false)
    {
		this->paramNode_ = std::make_shared<rclcpp::Node>(nodeName + "_param");
		this->paramClient_ = this->paramNode_->create_client<rcl_interfaces::srv::SetParametersAtomically>(SERVICE_NAME);
        this->_connToService(this->paramClient_);

		while (!this->exitF_)
		{
			std::string inputStr;
			std::cin >> inputStr;
			if (inputStr.size() <= 0)
				continue;
			
			auto param = rcl_interfaces::msg::Parameter();
			try
			{
				if (inputStr[0] == 's')
				{
					double val = std::stod(inputStr.substr(1));
					param.name = "samplingStep_ms";
					param.value.type = rcl_interfaces::msg::ParameterType::PARAMETER_DOUBLE;
					param.value.double_value = val;
				}
				else if (inputStr[0] == 'd')
				{
					double val = std::stod(inputStr.substr(1));
					param.name = "autoSaveTime_s";
					param.value.type = rcl_interfaces::msg::ParameterType::PARAMETER_DOUBLE;
					param.value.double_value = val;
				}
				else if (inputStr[0] == 'r')
				{
					double val = std::stod(inputStr.substr(1));
					param.name = "recordTime_s";
					param.value.type = rcl_interfaces::msg::ParameterType::PARAMETER_DOUBLE;
					param.value.double_value = val;
				}
				else
				{
					static bool recordF = true;
					param.name = "enabled_record";
					param.value.type = rcl_interfaces::msg::ParameterType::PARAMETER_BOOL;
					param.value.bool_value = recordF;
					recordF = !recordF;
				}

				this->_sendRequest(param);
			}
			catch (...)
			{
				std::cerr << "Caught Unknown Exception!" << std::endl;
			}

		}
    }

	~ParamsNode()
	{
		this->close();
	}

	void close()
	{
		if (this->exitF_)
			return;
		this->exitF_ = true;
	}
};


int main(int argc, char** argv)
{
    rclcpp::init(argc, argv);
    auto node = std::make_shared<ParamsNode>(NODE_NAME);
    rclcpp::spin(node);
    return 0;
}