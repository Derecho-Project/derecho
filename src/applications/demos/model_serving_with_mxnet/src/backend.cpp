#include <backend.hpp>
#include <utils.hpp>

#ifndef NDEBUG
#include <fstream>
#endif

namespace sospdemo {
#ifndef NDEBUG
    void Model::dump_to_file() const {
        // synset file
        std::fstream synset_fs("synset.dump", synset_fs.binary|synset_fs.trunc|synset_fs.out);
        synset_fs.write(reinterpret_cast<char*>(model_data.bytes),synset_size);
        synset_fs.close();

        // symbole file
        std::fstream symbol_fs("symbol.dump", symbol_fs.binary|symbol_fs.trunc|symbol_fs.out);
        symbol_fs.write(reinterpret_cast<char*>(model_data.bytes)+synset_size,symbol_size);
        symbol_fs.close();

        // params file
        std::fstream params_fs("params.dump", params_fs.binary|params_fs.trunc|params_fs.out);
        params_fs.write(reinterpret_cast<char*>(model_data.bytes)+synset_size+symbol_size,params_size);
        params_fs.close();

    }
#endif
    int InferenceEngine::load_model(const Model& model) {
        try{
            // 1 - load synset
            model.get_synset_vector(synset_vector);
    
            // 2 - load engine.
            {
                // load symbol
                this->net = mxnet::cpp::Symbol::LoadJSON(model.get_symbol_json());
                // load parameters
                std::map<std::string, mxnet::cpp::NDArray> parameters;
                model.get_parameters_map(&parameters);
                for (const auto &k : parameters) {
                    if (k.first.substr(0, 4) == "aux:") {
                        auto name = k.first.substr(4,k.first.size() - 4);
                        this->aux_map[name] = k.second.Copy(global_ctx);
                    } else if (k.first.substr(0, 4) == "arg:") {
                        auto name = k.first.substr(4,k.first.size() - 4);
                        this->args_map[name] = k.second.Copy(global_ctx);
                    }
                }
                mxnet::cpp::NDArray::WaitAll();
                this->args_map["data"] = mxnet::cpp::NDArray(input_shape, global_ctx, false, kFloat32);
                mxnet::cpp::Shape label_shape(input_shape[0]);
                this->args_map["softmax_label"] = mxnet::cpp::NDArray(label_shape, global_ctx, false);
    
                this->client_data = mxnet::cpp::NDArray(input_shape, global_ctx, false, kFloat32);
            }
            this->net.InferExecutorArrays(global_ctx, &arg_arrays, &grad_arrays, &grad_reqs, &aux_arrays,
                args_map, std::map<std::string, mxnet::cpp::NDArray>(), std::map<std::string, mxnet::cpp::OpReqType>(), aux_map);
            for (auto& i: grad_reqs) i = mxnet::cpp::OpReqType::kNullOp;
            this->executor_pointer.reset(new mxnet::cpp::Executor(net,global_ctx,arg_arrays,grad_arrays,grad_reqs,aux_arrays));
        } catch (...) {
            std::cerr << "Load model failed with unknown exception." << std::endl;
            return -1;
        }

        return 0;
    }

    InferenceEngine::InferenceEngine(Model& model):
        global_ctx(mxnet::cpp::Context::cpu()),
        input_shape(std::vector<mxnet::cpp::index_t>({1,3,224,224})){
        if (load_model(model) != 0) {
            std::cerr << "Failed to load model." << std::endl;
        }
    }

    InferenceEngine::~InferenceEngine() {
        // clean up the mxnet engine.
    }

    Guess InferenceEngine::inference(const Photo& photo) {
        Guess guess;
        // Here we assume that the photo is already in NDArray format to feed into the system
        // TODO: we should be able to accept normal picture format like jpeg and png
        std::vector<mxnet::cpp::NDArray> photo_ndarray = mxnet::cpp::NDArray::LoadFromBufferToList(
            static_cast<const void*>(photo.photo_data.bytes),photo.photo_data.size);
        photo_ndarray[0].CopyTo(&args_map["data"]);
        mxnet::cpp::NDArray::WaitAll();
        this->executor_pointer->Forward(false);
        mxnet::cpp::NDArray::WaitAll();
        // extract the result
        auto output_shape = executor_pointer->outputs[0].GetShape();
        mx_float max = -1e10;
        int idx = -1;
        for (unsigned int jj=0;jj<output_shape[1];jj++) {
            if (max < executor_pointer->outputs[0].At(0,jj)) {
                max = executor_pointer->outputs[0].At(0,jj);
                idx = static_cast<int>(jj);
            }
        }
        guess.guess = synset_vector[idx];
        guess.p = max;

        return std::move(guess);
    }

    BackEnd::~BackEnd() {
        // clean up.
    }

    Guess BackEnd::inference(const Photo& photo) {
#ifndef NDEBUG
        std::cout << "BackEnd::inference() called with photo tag = " << photo.tag << std::endl;
        std::cout.flush();
#endif//NDEBUG
        // 1 - load model if required
        std::shared_lock read_lock(inference_engines_mutex);
        if (inference_engines.find(photo.tag) == inference_engines.end()) {
            if (raw_models.find(photo.tag) == raw_models.end()) {
                Guess guess;
                std::cerr << "Cannot find model for photo tag:" << photo.tag << "." << std::endl;
                guess.guess = "Cannot find model for photo tag.";
                return guess;
            }
            read_lock.unlock();
            std::unique_ptr<InferenceEngine> engine = std::make_unique<InferenceEngine>(raw_models[photo.tag]);
            std::unique_lock write_lock(inference_engines_mutex);
            inference_engines[photo.tag] = std::move(engine);
            return inference_engines[photo.tag]->inference(photo);
        }

        // 2 - inference
        return inference_engines[photo.tag]->inference(photo);
    }

    int BackEnd::install_model(const uint32_t& tag,const ssize_t& synset_size,const ssize_t& symbol_size,const ssize_t& params_size,const BlobWrapper& model_data) {
        int ret = 0;
        auto& subgroup_handler = group->template get_subgroup<BackEnd>();
        // pass it to all replicas
        derecho::rpc::QueryResults<int> results = subgroup_handler.ordered_send<RPC_NAME(ordered_install_model)>(
            tag,synset_size,symbol_size,params_size,model_data);
        // check results
        decltype(results)::ReplyMap& replies = results.get();
        for(auto& reply_pair : replies) {
            int one_ret = reply_pair.second.get();
            std::cout << "Reply from node " << reply_pair.first << " is " << one_ret << std::endl;
			if (one_ret != 0) {
                ret = one_ret;
            }
        }
#ifndef NDEBUG
        std::cout << "Returning from BackEnd::install_model() with ret=" << ret << "." << std::endl;
        std::cout.flush();
#endif
        return ret;
    }

    int BackEnd::ordered_install_model(const uint32_t& tag,const ssize_t& synset_size,const ssize_t& symbol_size,const ssize_t& params_size,const BlobWrapper& model_data) {
        // validation
        if (raw_models.find(tag) != raw_models.end()) {
            std::cerr << "install_model failed because tag (" << tag << ") has been taken." << std::endl;
            return -1;
        }

        Model model;
        model.synset_size = synset_size;
        model.symbol_size = symbol_size;
        model.params_size = params_size;
        model.model_data = Blob(model_data.bytes,model_data.size);
        raw_models.emplace(tag,model); // TODO: recheck - unecessary copied is avoided?
#ifndef NDEBUG
        std::cout << "Returning from BackEnd::ordered_install_model() successfully." << std::endl;
        std::cout.flush();
#endif
        return 0;
    }

    int BackEnd::remove_model(const uint32_t& tag) {
        int ret = 0;
        auto& subgroup_handler = group->template get_subgroup<BackEnd>();
        // pass it to all replicas
        derecho::rpc::QueryResults<int> results = subgroup_handler.ordered_send<RPC_NAME(ordered_remove_model)>(tag);
        // check results;
        decltype(results)::ReplyMap& replies = results.get();
        for(auto& reply_pair : replies) {
            int one_ret = reply_pair.second.get();
            std::cout << "Reply from node " << reply_pair.first << " is " << one_ret << std::endl;
			if (one_ret != 0) {
                ret = one_ret;
            }
        }
        return ret;
    }

    int BackEnd::ordered_remove_model(const uint32_t& tag) {
        // remove from raw_model.
        auto model_search = raw_models.find(tag);
        if (model_search == raw_models.end()) {
            std::cerr << "remove_model failed because tag (" << tag << ") is not taken." << std::endl;
            return -1;
        }

        raw_models.erase(model_search);

        // remove from inference_engines
        std::unique_lock write_lock(inference_engines_mutex);
        auto engine_search = inference_engines.find(tag);
        if (engine_search != inference_engines.end()) {
            inference_engines.erase(engine_search);
        }

        return 0;
    }
}
