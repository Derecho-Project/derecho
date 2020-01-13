#include <iostream>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <chrono>
#include "simple_data_distribution_service.hpp"

#define CONF_DDS_DEMO_PRODUCER_TOPICS "DDS_DEMO/producer/topics"
#define CONF_DDS_DEMO_PRODUCER_FREQ_HZ "DDS_DEMO/producer/message_freq_hz"
#define CONF_DDS_DEMO_CONSUMER_TOPICS "DDS_DEMO/consumer/topics"

/**
   DDS Demo
   ========
 */

class ConsoleLogger : public IConnectionListener {
    const std::string prefix;
public:
    ConsoleLogger(const std::string _prefix): prefix(_prefix){}
    virtual void onData(const char* data, size_t len) {
        std::cout << prefix << " " << data << std::endl;
    }
    // This is not used yet.
    virtual void onError(uint64_t error_code) {
        std::cerr << prefix << " error code=0x" << std::hex
            << error_code << std::dec << std::endl;
    };
};

int main (int argc, char **argv) {
    std::cout << "Starting simple data distribution service (DDS)." << std::endl;
    DataDistributionService::initialize(argc,argv);
    std::atomic<bool> stopped = false;
    std::vector<std::thread> producer_threads;

    std::string producer_topics,consumer_topics;
    float producer_msg_freq = 0.0;
    // STEP 1. Print configuration information
    if (derecho::hasCustomizedConfKey(CONF_DDS_DEMO_PRODUCER_TOPICS)) {
        producer_topics = derecho::getConfString(CONF_DDS_DEMO_PRODUCER_TOPICS);
        producer_msg_freq = derecho::getConfFloat(CONF_DDS_DEMO_PRODUCER_FREQ_HZ);
        std::cout << "Producers publish to: " << producer_topics << std::endl;
        std::cout << "\tmessage rate: " << producer_msg_freq << " hertz" << std::endl;
    }
    if (derecho::hasCustomizedConfKey(CONF_DDS_DEMO_CONSUMER_TOPICS)) {
        consumer_topics = derecho::getConfString(CONF_DDS_DEMO_CONSUMER_TOPICS);
        std::cout << "Consumers subscribe to: " << consumer_topics << std::endl;
    }
    std::cout << "Once started, press any key to exit." << std::endl;

    // STEP 2. creating consumers and producers
    if (!consumer_topics.empty()) {
        std::istringstream iss(consumer_topics);
        std::string topic;
        while(std::getline(iss,topic,',')) {
            IMessageConsumer &c = DataDistributionService::getDDS().createConsumer(topic);
            c.setDataHandler(std::make_shared<ConsoleLogger>("consumer@"+topic));
        }
    }
    if (!producer_topics.empty()) {
        std::istringstream iss(producer_topics);
        std::string topic;
        while(std::getline(iss,topic,',')) {
            std::unique_ptr<IMessageProducer> producer_ptr = DataDistributionService::getDDS().createProducer(topic);
            producer_threads.emplace_back(std::thread([&](std::unique_ptr<IMessageProducer> p,std::string topic)->void{
                    std::chrono::milliseconds interval((int)(1000.0/producer_msg_freq));
                    auto wake_time = std::chrono::system_clock::now() + interval;
                    uint32_t counter = 1;
                    std::string prefix("msg-");
                    while(!stopped){
                        // produce
                        std::string data(prefix+std::to_string(counter));
                        p->write(data.c_str(),data.length()+1);
                        std::cout << "producer@" << topic << " " << data << std::endl;
                        std::this_thread::sleep_until(wake_time);
                        wake_time += interval;
                        counter ++;
                    }
                },std::move(producer_ptr),topic));
        }
    }
    // STEP 3. wait
    char x;
    std::cin >> x;
    stopped = true;
    for (auto &th : producer_threads) {
      th.join();
    }
    DataDistributionService::getDDS().close();

    return 0;
}
