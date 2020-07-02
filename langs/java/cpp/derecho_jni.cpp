#include <algorithm>
#include <cstdlib>
#include <derecho/core/derecho.hpp>
#include <initializer_list>  // For Args... args
#include <memory>
#include <mutex>
#include <string>
#include <time.h>
#include <typeindex>
#include <vector>

#include "bytes_object.hpp"
#include "derecho_jni.hpp"
#include "io_derecho_Group.h"
#include "io_derecho_QueryResults.h"
#include "io_derecho_Replicated.h"

/**
 * Derecho JNI Implementation
 */

#ifdef __CDT_PARSER__
#define REGISTER_RPC_FUNCTIONS(...)
#define RPC_NAME(...) 0ULL
#endif

/**
 * SubgroupObjectAgent is the universal C++ type class for all java subgroup 
 * types. C++ side of Derecho could use it to call Java functions using the 
 * java_function_call() interface. 
 */
struct SubgroupObjectAgent : public mutils::ByteRepresentable,
                             public derecho::GroupReference {
    derecho::subgroup_id_t sid;

    // Java virtual machine holder
    JavaVM* jvm;
    jint version;

private:
    /**
     * Get the environment pointer that belongs to the Java Virutal Machine.
     */
    JNIEnv* get_java_env() {
        JNIEnv* env;

        jint rs = jvm->GetEnv(reinterpret_cast<void**>(&env), version);
        if(rs == JNI_EDETACHED) {
            rs = jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);
        }
        assert(rs == JNI_OK);
        return env;
    }

public:
    //TODO implement the java object agent.

    /**
     * Constructors of SubgroupObjectAgent class.
    */
    SubgroupObjectAgent(derecho::subgroup_id_t usersid) : sid(usersid) { std::cout << "!*!*!NULL NULL" << std::endl; }

    SubgroupObjectAgent(derecho::subgroup_id_t usersid, JNIEnv* env) : sid(usersid), version(env->GetVersion()) {
        jint rs = env->GetJavaVM(&this->jvm);
        if(rs != JNI_OK) throw "JNI NOT OK";
    }

    /**
     * Creates a Java byte array from C++ Bytes object.
     */
    jobject create_java_arr_from_cpp_arr(JNIEnv* env, const derecho::Bytes& args) {
        char* arg = args.bytes;

        // 1. convert the args into a java byte array

        jbyteArray java_byte_array = env->NewByteArray(args.size);

        env->SetByteArrayRegion(java_byte_array, 0, args.size, reinterpret_cast<jbyte*>(arg));

        jbyte* buf = env->GetByteArrayElements(java_byte_array, NULL);

        // memcpy((char*)buf, arg, args.size);

        // 2. deserialize the java byte args array into a java object array

        jclass replicated_cls = env->FindClass("io/derecho/Replicated");
        jmethodID deserialize_mid = env->GetStaticMethodID(replicated_cls, "deserialize", "([B)Ljava/lang/Object;");
        jobject obj = env->CallStaticObjectMethod(replicated_cls, deserialize_mid, java_byte_array);

        env->ReleaseByteArrayElements(java_byte_array, buf, 0);

        return obj;
    }

    /**
     * Call Java functions from C++ side.
     * To call ordered_send() and p2p_send(), the JNI calls this interface and 
     * passes the Java object, parameters and return values from the RPC call 
     * here. The function deserializes these objects into Java, and then call 
     * the corresponding Java function through reflection on [cls_args].
     * The result of the function call is further serialized into a [Bytes] 
     * object and then passed through RPC call for further processing.
     */
    derecho::Bytes java_function_call(const std::string& name, const derecho::Bytes& cls_args, const derecho::Bytes& args) {
        try {
            // 0. prepare the java object to call

            JNIEnv* env = get_java_env();

            jclass subobj_cls = env->FindClass("io/derecho/SubgroupObjectRegistry");

            jmethodID subobj_mid = env->GetStaticMethodID(subobj_cls, "getInstance", "()Lio/derecho/SubgroupObjectRegistry;");

            jobject subreg = env->CallStaticObjectMethod(subobj_cls, subobj_mid);

            jmethodID getobj = env->GetMethodID(subobj_cls, "getObj", "(I)Ljava/lang/Object;");

            jobject realobj = env->CallObjectMethod(subreg, getobj, static_cast<jint>(sid));

            // 1. prepare java class object and get the method

            jobject cls_obj = create_java_arr_from_cpp_arr(env, cls_args);

            jobject arg_obj = create_java_arr_from_cpp_arr(env, args);

            // 2. call the Java function!

            jclass replicated_cls = env->FindClass("io/derecho/Replicated");
            jmethodID call_method_mid = env->GetStaticMethodID(replicated_cls, "callJavaMethod", "(Ljava/lang/Object;Ljava/lang/String;[Ljava/lang/Class;[Ljava/lang/Object;)Ljava/lang/Object;");

            jobject obj = env->CallStaticObjectMethod(replicated_cls, call_method_mid, realobj, env->NewStringUTF(name.c_str()), cls_obj, arg_obj);

            // 3. serialize the java object into a byte array

            jmethodID serialize_mid = env->GetStaticMethodID(replicated_cls, "serialize", "(Ljava/io/Serializable;)[B");
            jbyteArray java_byte_array = static_cast<jbyteArray>(env->CallStaticObjectMethod(replicated_cls, serialize_mid, obj));

            if(java_byte_array == NULL) return derecho::Bytes();

            // 4. convert the java byte array to a cpp string

            jbyte* buf = env->GetByteArrayElements(java_byte_array, NULL);
            jsize len_buf = env->GetArrayLength(java_byte_array);
            char* ret_buf = (char*)malloc((int)len_buf * sizeof(char));
            memcpy(ret_buf, (char*)buf, (int)len_buf);

            env->ReleaseByteArrayElements(java_byte_array, buf, 0);

            jint rs = jvm->DetachCurrentThread();

            if(rs == 0) throw "JNI NOT OK";

            return derecho::Bytes(ret_buf, (int)len_buf);

        } catch(const std::exception& e) {
            std::cout << "Exception!!!" << e.what() << std::endl;
            exit(0);
        }
    }

    // TODO: Need to change default behavior by serialize and deserialize java object corresponding to SID
    DEFAULT_SERIALIZATION_SUPPORT(SubgroupObjectAgent, sid);

    REGISTER_RPC_FUNCTIONS(SubgroupObjectAgent, java_function_call);
};

/**
 * Alias for group type in JNI implementation.
 */
typedef derecho::Group<SubgroupObjectAgent> jni_group;

/**
 * CallbackSetWrapper wraps java callbacks in C/C++. 
 */
class CallbackSetWrapper {
    // java virtual machine holder
    JavaVM* jvm;
    jint version;
    // the java side callbacks.
    jweak callback_set;

private:
    JNIEnv* get_java_env() {
        JNIEnv* env;
        jint rs = jvm->GetEnv(reinterpret_cast<void**>(&env), version);
        if(rs == JNI_EDETACHED) {
            rs = jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);
        }
        assert(rs == JNI_OK);
        return env;
    }

public:
    // The constructor of CallbackSetWrapper.
    CallbackSetWrapper(JNIEnv* env, jobject _callback_set) : version(env->GetVersion()),
                                                             callback_set(_callback_set) {
        jint rs = env->GetJavaVM(&this->jvm);
        if(rs != JNI_OK) throw "JNI NOT OK";

        // get java callback mids:
        if(callback_set != NULL) {
            // create a weak global reference to callback_set
            callback_set = env->NewWeakGlobalRef(callback_set);
        }
    }
    // destructor
    virtual ~CallbackSetWrapper() {
        JNIEnv* env = get_java_env();

        if(callback_set != NULL) {
            env->DeleteWeakGlobalRef(callback_set);
        }

        // jint rs = jvm->DetachCurrentThread();
        // assert(rs == 0);
    }
    // global_stability_callback
    void global_stability_callback(derecho::subgroup_id_t sid, node_id_t nid, derecho::message_id_t message_id, std::optional<std::pair<char*, long long int>> message, persistent::version_t ver) {
        if(this->callback_set == NULL) {
            // skip null callback set.
            return;
        }

        JNIEnv* env = get_java_env();
        jobject byte_buffer = NULL;
        if(message) {
            byte_buffer = env->NewDirectByteBuffer(static_cast<void*>(std::get<0>(message.value())),
                                                   static_cast<jlong>(std::get<1>(message.value())));
        };
        jclass callback_set_class = env->GetObjectClass(callback_set);
        jmethodID global_stability_callback_mid = env->GetMethodID(callback_set_class, "global_stability_callback", "(IIJJLjava/nio/ByteBuffer;)V");
        env->CallVoidMethod(this->callback_set,
                            global_stability_callback_mid,
                            static_cast<jint>(sid),
                            static_cast<jint>(nid),
                            static_cast<jlong>(message_id),
                            static_cast<jlong>(ver),
                            byte_buffer);
        jint rs = jvm->DetachCurrentThread();
        if(rs != JNI_OK) throw "JNI NOT OK";
    }
    // local_persistent_callback
    void local_persistence_callback(derecho::subgroup_id_t sid, persistent::version_t ver) {
        if(this->callback_set == NULL) {
            // skip null callback_set.
            return;
        }

        JNIEnv* env = get_java_env();
        jclass callback_set_class = env->GetObjectClass(callback_set);
        jmethodID local_persistence_callback_mid = env->GetMethodID(callback_set_class, "local_persistence_callback", "(IJ)V");
        env->CallVoidMethod(this->callback_set,
                            local_persistence_callback_mid,
                            static_cast<jint>(sid),
                            static_cast<jlong>(ver));
        jint rs = jvm->DetachCurrentThread();
        if(rs != JNI_OK) throw "JNI NOT OK";
    }
    // global_persistent_ballback
    void global_persistence_callback(derecho::subgroup_id_t sid, persistent::version_t ver) {
        if(this->callback_set == NULL) {
            // skip null callback_set.
            return;
        }

        JNIEnv* env = get_java_env();
        jclass callback_set_class = env->GetObjectClass(callback_set);
        jmethodID global_persistence_callback_mid = env->GetMethodID(callback_set_class, "global_persistence_callback", "(IJ)V");
        env->CallVoidMethod(this->callback_set,
                            global_persistence_callback_mid,
                            static_cast<jint>(sid),
                            static_cast<jlong>(ver));
        jint rs = jvm->DetachCurrentThread();
        if(rs != JNI_OK) throw "JNI NOT OK";
    }
};

/** 
 * GroupHolder stores the handle for a group.
 */
class GroupHolder {
    // the unique pointer hold the ownership a group.
    std::unique_ptr<jni_group> group;
    // callback wrapper
    std::unique_ptr<CallbackSetWrapper> callback_set;

public:
    // constructor, _group is moved to this->group.
    GroupHolder(std::unique_ptr<jni_group>&& _group, std::unique_ptr<CallbackSetWrapper>&& _callback_set) : group(std::move(_group)),
                                                                                                            callback_set(std::move(_callback_set)) {}

    // get group pointer from java handle
    jni_group* get_group() {
        return group.get();
    }
};

/** 
 * QueryResultHolder stores the handle for the return value of QueryResults object.
 */
class QueryResultHolder {
    // the unique pointer to hold Query Result future of C++.
    derecho::rpc::QueryResults<derecho::Bytes> query_result;

public:
    // constructor, _query_result is moved to this->query_result.
    QueryResultHolder(derecho::rpc::QueryResults<derecho::Bytes>& _query_result) : query_result(std::move(_query_result)) {}

    // get query_result pointer from java handle
    derecho::rpc::QueryResults<derecho::Bytes>* get_result() {
        return &query_result;
    }
};

// get group holder from the Java handle
auto get_group_holder(JNIEnv* env, jobject jgroup) {
    jclass group_cls = env->GetObjectClass(jgroup);
    jfieldID handle_fid = env->GetFieldID(group_cls, "handle", "J");
    jlong jhandle = env->GetLongField(jgroup, handle_fid);

    return reinterpret_cast<GroupHolder*>(jhandle);
}

////////////////////////////////////////////////////////////////////////
// helper functions
/**
 * create a java view from c/c++ view
 */
jobject create_java_view_from_cpp_view(JNIEnv* env, const derecho::View* view) {
    // check if view is null.
    if(view == nullptr) {
        return static_cast<jobject>(NULL);
    }
    // 1 - create new object
    jclass view_class = env->FindClass("io/derecho/View");
    jclass list_cls = env->FindClass("java/util/List");
    jmethodID list_add = env->GetMethodID(list_cls, "add", "(Ljava/lang/Object;)Z");
    jmethodID view_constructor = env->GetMethodID(view_class, "<init>", "()V");
    jobject view_object = env->NewObject(view_class, view_constructor);
    // 2 - fill the object
    jfieldID fid;
    // 2.1 - int viewId;
    if((fid = env->GetFieldID(view_class, "viewId", "I")) == 0) {
        std::cerr << "failed to find viewId" << std::endl;
        std::cerr.flush();
        return static_cast<jobject>(NULL);
    } else {
        env->SetIntField(view_object, fid, view->vid);
    }
    // 2.2 - public List<Integer> members;
    if((fid = env->GetFieldID(view_class, "members", "Ljava/util/List;")) == 0) {
        std::cerr << "failed to find members" << std::endl;
        std::cerr.flush();
        return static_cast<jobject>(NULL);
    } else {
        jobject members_jobj = env->GetObjectField(view_object, fid);

        for(auto& member : view->members) {
            jclass integer_class = env->FindClass("java/lang/Integer");
            jmethodID integer_constructor = env->GetMethodID(integer_class, "<init>", "(I)V");  // changed init integer_class
            jobject integer_object = env->NewObject(integer_class, integer_constructor, static_cast<jint>(member));
            env->CallBooleanMethod(members_jobj, list_add, integer_object);
        }
    }

    // 2.3 - int myRank;
    if((fid = env->GetFieldID(view_class, "myRank", "I")) == 0) {
        std::cerr << "failed to find myRank" << std::endl;
        std::cerr.flush();
        return static_cast<jobject>(NULL);
    } else {
        env->SetIntField(view_object, fid, view->my_rank);
    }

    // 2.4 - boolean isAdequatelyProvisioned;
    if((fid = env->GetFieldID(view_class, "isAdequatelyProvisioned", "Z")) == 0) {
        std::cerr << "failed to find isAdequatelyProvisioned" << std::endl;
        std::cerr.flush();
        return static_cast<jobject>(NULL);
    } else {
        env->SetBooleanField(view_object, fid, view->is_adequately_provisioned);
    }
    // 2.5 - List<Boolean> failed;
    if((fid = env->GetFieldID(view_class, "failed", "Ljava/util/List;")) == 0) {
        std::cerr << "failed to find failed" << std::endl;
        std::cerr.flush();
        return static_cast<jobject>(NULL);
    } else {
        jobject failed_jobj = env->GetObjectField(view_object, fid);

        for(auto& fail : view->failed) {
            jclass boolean_class = env->FindClass("java/lang/Boolean");
            jmethodID boolean_constructor = env->GetMethodID(boolean_class, "<init>", "(Z)V");
            jobject boolean_object = env->NewObject(boolean_class, boolean_constructor, static_cast<jboolean>(fail));
            env->CallBooleanMethod(failed_jobj, list_add, boolean_object);
        }
    }
    // 2.6 - int numFailed;
    if((fid = env->GetFieldID(view_class, "numFailed", "I")) == 0) {
        std::cerr << "failed to find numFailed" << std::endl;
        std::cerr.flush();
        return static_cast<jobject>(NULL);
    } else
        env->SetIntField(view_object, fid, view->num_failed);
    // --- subgroup ids in the view is not used.
    //    // 2.7 - List<Integer> subgroupIDs;
    //    if((fid = env->GetFieldID(view_class, "subgroupIDs", "Ljava/util/List;")) == 0) {
    //        std::cout << "failed to find subgroupIDs" << std::endl;
    //        std::cout.flush();
    //        return static_cast<jobject>(NULL);
    //    } else {
    //        jobject subgroupids_jobj = env->GetObjectField(view_object, fid);
    //
    //        for(auto it: view->subgroup_ids_by_type_id.at(0)) {
    //            jclass integer_class = env->FindClass("java/lang/Integer");
    //            jmethodID integer_constructor = env->GetMethodID(integer_class, "<init>", "(I)V");
    //            jobject integer_object = env->NewObject(integer_class, integer_constructor, static_cast<jint>(it));
    //            env->CallBooleanMethod(subgroupids_jobj, list_add, integer_object);
    //        }
    //    }

    // 3 - return the view
    return view_object;
}

// bugfix: inner list has a different type, whose iterator has a different list_iterator_mid
derecho::subgroup_shard_layout_t vectorize_helper(JNIEnv* env, jobject outer_list, derecho::View& curr_view) {
    derecho::subgroup_shard_layout_t subgroup_shard_layout;

    jclass outer_list_class, outer_list_iterator_class;
    jclass inner_list_class, inner_list_iterator_class;
    jclass set_class, set_iterator_class;
    jmethodID outer_list_iterator_mid, outer_list_iterator_has_next_mid, outer_list_iterator_next_mid;
    jmethodID inner_list_iterator_mid, inner_list_iterator_has_next_mid, inner_list_iterator_next_mid;
    jmethodID set_iterator_mid, set_iterator_has_next_mid, set_iterator_next_mid;

    outer_list_class = env->GetObjectClass(outer_list);

    if((outer_list_iterator_mid = env->GetMethodID(outer_list_class, "listIterator", "()Ljava/util/ListIterator;")) == 0) {
        std::cerr << "could not find list iterator" << std::endl;
        std::cerr.flush();
        return std::vector<std::vector<derecho::SubView>>();
    }
    jobject outer_list_iterator = env->CallObjectMethod(outer_list, outer_list_iterator_mid);
    outer_list_iterator_class = env->GetObjectClass(outer_list_iterator);
    if((outer_list_iterator_has_next_mid = env->GetMethodID(outer_list_iterator_class, "hasNext", "()Z")) == 0) {
        std::cerr << "could not find has next list iterator" << std::endl;
        std::cerr.flush();
        return std::vector<std::vector<derecho::SubView>>();
    }
    if((outer_list_iterator_next_mid = env->GetMethodID(outer_list_iterator_class, "next", "()Ljava/lang/Object;")) == 0) {
        std::cerr << "Could not find next list iterator" << std::endl;
        std::cerr.flush();
        return std::vector<std::vector<derecho::SubView>>();
    }
    while((bool)(env->CallBooleanMethod(outer_list_iterator, outer_list_iterator_has_next_mid))) {
        jobject inner_list = env->CallObjectMethod(outer_list_iterator, outer_list_iterator_next_mid);
        inner_list_class = env->GetObjectClass(inner_list);
        inner_list_iterator_mid = env->GetMethodID(inner_list_class, "listIterator", "()Ljava/util/ListIterator;");
        jobject inner_list_iterator = env->CallObjectMethod(inner_list, inner_list_iterator_mid);
        inner_list_iterator_class = env->GetObjectClass(inner_list_iterator);
        inner_list_iterator_has_next_mid = env->GetMethodID(inner_list_iterator_class, "hasNext", "()Z");
        inner_list_iterator_next_mid = env->GetMethodID(inner_list_iterator_class, "next", "()Ljava/lang/Object;");

        std::vector<derecho::SubView> subgroup;
        while((bool)(env->CallBooleanMethod(inner_list_iterator, inner_list_iterator_has_next_mid))) {
            jobject set = env->CallObjectMethod(inner_list_iterator, inner_list_iterator_next_mid);
            set_class = env->GetObjectClass(set);
            set_iterator_mid = env->GetMethodID(set_class, "iterator", "()Ljava/util/Iterator;");
            jobject set_iterator = env->CallObjectMethod(set, set_iterator_mid);
            set_iterator_class = env->GetObjectClass(set_iterator);
            set_iterator_has_next_mid = env->GetMethodID(set_iterator_class, "hasNext", "()Z");
            set_iterator_next_mid = env->GetMethodID(set_iterator_class, "next", "()Ljava/lang/Object;");

            //derecho::SubView sub_view;
            std::vector<node_id_t> shard;
            while((bool)(env->CallBooleanMethod(set_iterator, set_iterator_has_next_mid))) {
                jobject node_id_jobj = env->CallObjectMethod(set_iterator, set_iterator_next_mid);
                jclass integer_class = env->GetObjectClass(node_id_jobj);
                jmethodID int_value_mid = env->GetMethodID(integer_class, "intValue", "()I");
                jint node_id = env->CallIntMethod(node_id_jobj, int_value_mid);
                shard.push_back(node_id);
            }
            subgroup.emplace_back(curr_view.make_subview(shard, derecho::Mode::ORDERED));
        }
        subgroup_shard_layout.emplace_back(subgroup);
    }
    return subgroup_shard_layout;
}

derecho::subgroup_allocation_map_t generate_shard_view(JNIEnv* env,
                                                       jobject shard_view_generator, jobject list_of_classes,
                                                       const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
    derecho::subgroup_allocation_map_t subgroup_allocation;
    // 1 - call shard_view_generator to create a view
    // if return value is a null object, throw an exception as follows:
    // throw derecho::subgroup_provisioning_exception();
#ifndef NDEBUG
    std::cout << "generated_shard_view is called. number of node = " << curr_view.members.size() << std::endl;
    std::cout.flush();
#endif

    jclass svgcls = env->GetObjectClass(shard_view_generator);
    jobject jsam;
    jmethodID mid, get_value_mid;
    if(svgcls == NULL) {
        std::cerr << "list_of_classes is empty" << std::endl;
        std::cerr.flush();
        throw "Class is NULL";
    }
    if((mid = env->GetMethodID(svgcls, "generate", "(Ljava/util/List;Lio/derecho/View;Lio/derecho/View;)Ljava/util/Map;")) == 0) {
        throw derecho::subgroup_provisioning_exception();
    } else {
        jsam = env->CallObjectMethod(shard_view_generator, mid, list_of_classes,
                                     create_java_view_from_cpp_view(env, prev_view.get()),
                                     create_java_view_from_cpp_view(env, &curr_view));
        if(jsam == NULL) {
            std::cerr << "java shard_view_generator returns NULL." << std::endl;
            std::cerr.flush();
            throw derecho::subgroup_provisioning_exception();
        }
    }

    // 2 - generate the subgroup_allocation_map_t object
    // please see
    // https://github.com/Derecho-Project/derecho/blob/master/include/derecho/core/subgroup_info.hpp
    // for the definition of subgroup_allocation_map_t
    jclass listcls = env->GetObjectClass(list_of_classes);

    jmethodID mGet = env->GetMethodID(listcls, "get", "(I)Ljava/lang/Object;");
    jmethodID mSize = env->GetMethodID(listcls, "size", "()I");

    if(mSize == NULL || mGet == NULL) {
        throw derecho::subgroup_provisioning_exception();
    }

    jint size = env->CallIntMethod(list_of_classes, mSize);

    jclass mapcls = env->GetObjectClass(jsam);

    for(jint i = 0; i < size; i++) {
        jobject entry = (jobject)env->CallObjectMethod(list_of_classes, mGet, i);

        if((get_value_mid = env->GetMethodID(mapcls, "get", "(Ljava/lang/Object;)Ljava/lang/Object;")) == 0) {
            derecho::subgroup_allocation_map_t map;
            return map;
        }
        jobject value = env->CallObjectMethod(jsam, get_value_mid, entry);

        auto new_subgroups = vectorize_helper(env, value, curr_view);

        auto& curr_allocation = subgroup_allocation[typeid(SubgroupObjectAgent)];  // reference because of modification

        if(curr_allocation.empty())
            curr_allocation = std::move(new_subgroups);
        else
            curr_allocation.insert(curr_allocation.end(), std::make_move_iterator(new_subgroups.begin()), std::make_move_iterator(new_subgroups.end()));
    }

#ifndef NDEBUG
/****
    { // dump subgroup_allocation
        // using subgroup_allocation_map_t = std::map<std::type_index, subgroup_shard_layout_t>;
        // using subgroup_shard_layout_t = std::vector<std::vector<SubView>>;
        std::cout << "dump subgroup_allocation information:" << std::endl;
        for(auto& ent : subgroup_allocation) {
            std::cout << "type_index:" << ent.first.name() << std::endl;
            std::cout << "subgroup_shard_layout: {" << std::endl;
            for(auto& subgroup_ent : ent.second) {
                std::cout << "\t{" << std::endl;
                for(auto& shard_ent : subgroup_ent) {
                    std::cout << "\t\t{" << std::endl;
                    std::cout << "\t\t\t";
                    for(auto& nid : shard_ent.members) {
                        std::cout << nid << ",";
                    }
                    std::cout << std::endl;
                    std::cout << "\t\t}" << std::endl;
                }
                std::cout << "\t}" << std::endl;
            }
            std::cout << "}" << std::endl;
        }
    }
    ****/
#endif

    return std::move(subgroup_allocation);
}

/*
 * Class:     io_derecho_Group
 * Method:    createGroup
 * Signature: (Ljava/util/List;Lio/derecho/IShardViewGenerator;Lio/derecho/Group/ICallbackSet;)J
 */
JNIEXPORT jlong JNICALL Java_io_derecho_Group_createGroup(JNIEnv* env, jobject self, jobject list_of_classes, jobject shard_view_generator, jobject callback_set) {
    // The mapping between derecho group types and subgroup id is like this:
    // derecho::Group<T1,T2,T3> group ...
    // Suppose we have 3 subgroups of T1, 2 subgroups of T2, and 4 subgroups of T3. Then,
    // derecho will hold a type->subgroup_id mapping like this:
    // T1->(0,1,2); T2->(3,4); T3->(5,6,7,8)
    // please check "ViewManager::make_subgroup_maps()" for more information.
    //
    // In the java case, we have only one C++ type. Therefore, the subgroup ids follow the order of
    // the subgroup provided in the order provided by shard_view_generator.
    // For example, if the list_of_classes feed to shard_view_generator is
    // [Java_Type_1,Java_Type_2,Java_Type_3,Java_Type_1,Java_Type_2]
    // Then, we will have a type->subgroup_id mapping like this:
    // SubgroupObjectAgent -> (0,1,2,3,4,5);
    // And in the java object registry, which maps subgroup id to corresponding the java subgroup object, we have
    // 0 -> java object of Java_Type_1,
    // 1 -> java object of Java_Type_2,
    // 2 -> java object of Java_Type_3,
    // 3 -> java object of Java_Type_1,
    // 4 -> java object of Java_Type_2,
    //

    const derecho::shard_view_generator_t subgroup_info = [env, shard_view_generator, list_of_classes](const std::vector<std::type_index>& subgroup_type_order,
                                                                                                       const std::unique_ptr<derecho::View>& prev_view,
                                                                                                       derecho::View& curr_view) {
        // 1 - prepare prev_view and curr_view in java format
        jobject curr_view_jobject = create_java_view_from_cpp_view(env, &curr_view);
        if(curr_view_jobject == NULL) {  // prev_view could be NULL, but that's fine.
            throw derecho::subgroup_provisioning_exception();
        }
        // 2 - call java ShardViewGenerator to create view
        return generate_shard_view(env, shard_view_generator, list_of_classes, prev_view, curr_view);
    };

    derecho::Factory<SubgroupObjectAgent> factory = [list_of_classes, env](persistent::PersistentRegistry*, derecho::subgroup_id_t subgroup_id) {
        jclass listcls = env->GetObjectClass(list_of_classes);
        jmethodID list_mid = env->GetMethodID(listcls, "get", "(I)Ljava/lang/Object;");
        jobject javaclssobj = env->CallObjectMethod(list_of_classes, list_mid, subgroup_id);
        jclass javaclscls = env->GetObjectClass(javaclssobj);
        jmethodID getname = env->GetMethodID(javaclscls, "getName", "()Ljava/lang/String;");
        jstring javaclsname = static_cast<jstring>(env->CallObjectMethod(javaclssobj, getname));
        const char* str = env->GetStringUTFChars(javaclsname, NULL);
        std::string str_str(str);
        std::replace(str_str.begin(), str_str.end(), '.', '/');

        //printf("******* Subgroup Object Agent Factory is called at subgroup ID %d with class %s ********\n", subgroup_id, str_str.c_str());

        //TODO: convert jobject to jclass and init
        jclass javacls = env->FindClass(str_str.c_str());

        jmethodID obj_mid = env->GetMethodID(javacls, "<init>", "()V");
        jobject java_obj = env->NewObject(javacls, obj_mid);
        // INitialized object from class not sure about constructor arguements or if path to class is correct.

        jclass registrycls = env->FindClass("io/derecho/SubgroupObjectRegistry");
        jmethodID getinstance_mid = env->GetStaticMethodID(registrycls, "getInstance", "()Lio/derecho/SubgroupObjectRegistry;");

        jobject registryobj = env->CallStaticObjectMethod(registrycls, getinstance_mid);
        jmethodID add_pair_mid = env->GetMethodID(registrycls, "addSidTypePair", "(ILjava/lang/Object;)V");
        env->CallObjectMethod(registryobj, add_pair_mid, static_cast<jint>(subgroup_id), java_obj);

        return std::make_unique<SubgroupObjectAgent>(subgroup_id, env);
    };

    CallbackSetWrapper* callback_set_wrapper = new CallbackSetWrapper(env, callback_set);

    derecho::CallbackSet stability_callbacks = {
            [callback_set_wrapper](derecho::subgroup_id_t sid, node_id_t nid, derecho::message_id_t message_id, std::optional<std::pair<char*, long long int>> message, persistent::version_t ver) {
                callback_set_wrapper->global_stability_callback(sid, nid, message_id, message, ver);
            },
            [callback_set_wrapper](derecho::subgroup_id_t sid, persistent::version_t ver) {
                callback_set_wrapper->local_persistence_callback(sid, ver);
            },
            [callback_set_wrapper](derecho::subgroup_id_t sid, persistent::version_t ver) {
                callback_set_wrapper->global_persistence_callback(sid, ver);
            }};

    std::vector<derecho::view_upcall_t> view_upcalls = {};

    GroupHolder* group_holder = new GroupHolder(
            std::make_unique<derecho::Group<SubgroupObjectAgent>>(
                    stability_callbacks,                   // stability callbacks
                    derecho::SubgroupInfo(subgroup_info),  // subgroup info
                    nullptr,                               // deseriailziation context
                    view_upcalls,                          // view upcall: TODO: change the view in Group...
                    factory),
            std::unique_ptr<CallbackSetWrapper>(callback_set_wrapper)  // hold the callback_set wrapper for destructor.
    );

    return reinterpret_cast<jlong>(group_holder);
}

/*
 * Class:     io_derecho_Group
 * Method:    barrierSync
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_io_derecho_Group_barrierSync(JNIEnv* env, jobject jgroup) {
    get_group_holder(env, jgroup)->get_group()->barrier_sync();
    return;
}

/*
 * Class:     io_derecho_Group
 * Method:    leave
 * Signature: (Z)V
 */
JNIEXPORT void JNICALL Java_io_derecho_Group_leave(JNIEnv* env, jobject jgroup, jboolean is_shutdown) {
    // leave group
    get_group_holder(env, jgroup)->get_group()->leave(static_cast<bool>(is_shutdown));
    // descroty group
    delete get_group_holder(env, jgroup);
    return;
}

/*
 * Class:     io_derecho_Group
 * Method:    get_my_rank
 * Signature: ()Ljava/lang/Integer;
 */
JNIEXPORT jobject JNICALL Java_io_derecho_Group_get_1my_1rank(JNIEnv* env, jobject jgroup) {
    std::int32_t res = get_group_holder(env, jgroup)->get_group()->get_my_rank();
    jclass integer_cls = env->FindClass("java/lang/Integer");
    jmethodID init_mid = env->GetMethodID(integer_cls, "<init>", "(I)V");
    jobject integer_obj = env->NewObject(integer_cls, init_mid, static_cast<jint>(res));
    return integer_obj;
}

/*
 * Class:     io_derecho_Group
 * Method:    getMembers
 * Signature: ()Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_io_derecho_Group_getMembers(JNIEnv* env, jobject jgroup) {
    std::vector<node_id_t> members = get_group_holder(env, jgroup)->get_group()->get_members();
    jclass list_cls = env->FindClass("java/util/List");
    jmethodID list_add_mid = env->GetMethodID(list_cls, "add", "(Ljava/lang/Object;)Z");
    jclass arr_list_cls = env->FindClass("java/util/ArrayList");
    jmethodID arr_init_mid = env->GetMethodID(arr_list_cls, "<init>", "()V");
    jobject arr_obj = env->NewObject(arr_list_cls, arr_init_mid);
    jclass integer_cls = env->FindClass("java/lang/Integer");
    jmethodID integer_init_mid = env->GetMethodID(integer_cls, "<init>", "(I)V");
    for(node_id_t id : members) {
        jobject int_obj = env->NewObject(integer_cls, integer_init_mid, id);
        env->CallObjectMethod(arr_obj, list_add_mid, int_obj);
    }
    return arr_obj;
}

/*
 * Class:     io_derecho_Replicated
 * Method:    send
 * Signature: (JLio/derecho/IMessageGenerator;)V
 */
JNIEXPORT void JNICALL Java_io_derecho_Replicated_send(JNIEnv* env, jobject replicated_obj, jlong payload_size, jobject message_generator) {
    //Step 1: get the group handle and sid from the replicated object
    jclass replicated_cls = env->GetObjectClass(replicated_obj);
    jfieldID sid_fid = env->GetFieldID(replicated_cls, "sid", "I");
    int subgroup_id = static_cast<int>(env->GetIntField(replicated_obj, sid_fid));

    jfieldID group_handle_fid = env->GetFieldID(replicated_cls, "group_handle", "J");
    long group_handle = static_cast<long>(env->GetLongField(replicated_obj, group_handle_fid));

    //Step 2: call group.get_subgroup<SubgroupObjectRegistry>(subgroup index)
    jni_group* group = reinterpret_cast<GroupHolder*>(group_handle)->get_group();
    derecho::Replicated<SubgroupObjectAgent>& repl_subgroup = group->get_subgroup<SubgroupObjectAgent>(subgroup_id);

    //Step 4: call send on this object
    long payload = static_cast<long>(payload_size);
    auto lambda = [env, message_generator, replicated_obj](char* buff) {
        jclass message_gen_cls = env->GetObjectClass(message_generator);
        jmethodID write_mid = env->GetMethodID(message_gen_cls, "write", "(Ljava/nio/ByteBuffer;)V");

        // wrap C buffer into Byte Buffer
        auto max_payload_size = derecho::getConfUInt64(CONF_SUBGROUP_DEFAULT_MAX_PAYLOAD_SIZE);
        jobject bb = env->NewDirectByteBuffer(static_cast<void*>(buff), static_cast<jlong>(max_payload_size));

        //Calling write method
        env->CallVoidMethod(message_generator, write_mid, bb);
    };

    repl_subgroup.send(payload, lambda);
}

derecho::Bytes create_cpp_bytes_from_java_byte_array(JNIEnv* env, jbyteArray args) {
    jbyte* buff_ptr = env->GetByteArrayElements(args, NULL);

    jsize buff_size = env->GetArrayLength(args);

    int temp_size = ((int)buff_size) * sizeof(char);

    char* cpp_buf = (char*)malloc(temp_size);

    memcpy(cpp_buf, (char*)buff_ptr, temp_size);

    env->ReleaseByteArrayElements(args, buff_ptr, 0);

    derecho::Bytes ret_val(cpp_buf, (int)buff_size);

    return ret_val;
}

/*
 * Class:     io_derecho_Replicated
 * Method:    ordered_send
 * Signature: (Ljava/lang/String;[B[B)J
 */
JNIEXPORT jlong JNICALL Java_io_derecho_Replicated_ordered_1send(JNIEnv* env, jobject replicated_obj, jstring func_name, jbyteArray class_args, jbyteArray args) {
    //Step 1: get the group handle and sid from the replicated object

    jclass replicated_cls = env->GetObjectClass(replicated_obj);
    jfieldID sid_fid = env->GetFieldID(replicated_cls, "sid", "I");
    int subgroup_id = static_cast<int>(env->GetIntField(replicated_obj, sid_fid));

    jfieldID group_handle_fid = env->GetFieldID(replicated_cls, "group_handle", "J");
    long group_handle = static_cast<long>(env->GetLongField(replicated_obj, group_handle_fid));

    //Step 2: call group.get_subgroup<SubgroupObjectRegistry>(subgroup index)

    jni_group* group = reinterpret_cast<GroupHolder*>(group_handle)->get_group();
    derecho::Replicated<SubgroupObjectAgent>& repl_subgroup = group->get_subgroup<SubgroupObjectAgent>(subgroup_id);

    //Step 3: call ordered_send on this object

    const char* real_func_name = env->GetStringUTFChars(func_name, NULL);

    // 3.1 convert the Java array as a C++ byte array

    derecho::Bytes cpp_real_bytes = create_cpp_bytes_from_java_byte_array(env, args);
    derecho::Bytes cpp_real_class_args = create_cpp_bytes_from_java_byte_array(env, class_args);

    // 3.2 call the ordered_send!

    std::string cpp_func_name(real_func_name);

    derecho::rpc::QueryResults<derecho::Bytes> results = repl_subgroup.ordered_send<RPC_NAME(java_function_call)>(cpp_func_name, cpp_real_class_args, cpp_real_bytes);

    QueryResultHolder* qrh = new QueryResultHolder(results);

    //CLose handle method for JNI

    return reinterpret_cast<jlong>(qrh);
}

/*
 * Class:     io_derecho_Replicated
 * Method:    p2p_send
 * Signature: (IILjava/lang/String;[B[B)J
 */
JNIEXPORT jlong JNICALL Java_io_derecho_Replicated_p2p_1send(JNIEnv* env, jobject replicated_obj, jint target_node_id, jint target_subgroup_id, jstring func_name, jbyteArray class_args, jbyteArray args) {
    // this copies and pasted ordered_send. Intended to reduce overhead by not factoring stuff out.

    //Step 1: get the group handle and sid from the replicated object

    jclass replicated_cls = env->GetObjectClass(replicated_obj);

    jfieldID group_handle_fid = env->GetFieldID(replicated_cls, "group_handle", "J");
    long group_handle = static_cast<long>(env->GetLongField(replicated_obj, group_handle_fid));

    //Step 2: call group.get_non_member_subgroup<SubgroupObjectRegistry>(subgroup index)

    jni_group* group = reinterpret_cast<GroupHolder*>(group_handle)->get_group();
    derecho::ExternalCaller<SubgroupObjectAgent>& repl_subgroup = group->get_nonmember_subgroup<SubgroupObjectAgent>(static_cast<int>(target_subgroup_id));

    //Step 3: call p2p_send on this object

    const char* real_func_name = env->GetStringUTFChars(func_name, NULL);

    std::string cpp_func_name(real_func_name);

    derecho::Bytes cpp_real_bytes = create_cpp_bytes_from_java_byte_array(env, args);
    derecho::Bytes cpp_real_class_args = create_cpp_bytes_from_java_byte_array(env, class_args);

    derecho::rpc::QueryResults<derecho::Bytes> results = repl_subgroup.p2p_send<RPC_NAME(java_function_call)>(static_cast<node_id_t>(target_node_id), cpp_func_name, cpp_real_class_args, cpp_real_bytes);

    QueryResultHolder* qrh = new QueryResultHolder(results);

    return reinterpret_cast<jlong>(qrh);
}

/*
 * Class:     io_derecho_QueryResults
 * Method:    getReplyMapOS
 * Signature: (J)Ljava/util/Map;
 */
JNIEXPORT jobject JNICALL Java_io_derecho_QueryResults_getReplyMapOS(JNIEnv* env, jobject qr_object, jlong handle) {
    // Process the query results into a java map
    jclass hash_map_cls = env->FindClass("java/util/HashMap");

    jmethodID hash_map_constructor = env->GetMethodID(hash_map_cls, "<init>", "()V");
    jobject hash_map_object = env->NewObject(hash_map_cls, hash_map_constructor);

    jmethodID map_put = env->GetMethodID(hash_map_cls, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

    QueryResultHolder* qrh = reinterpret_cast<QueryResultHolder*>(handle);
    derecho::rpc::QueryResults<derecho::Bytes>* result = qrh->get_result();

    for(auto& reply_pair : result->get()) {
        jclass integer_class = env->FindClass("java/lang/Integer");
        jmethodID integer_constructor = env->GetMethodID(integer_class, "<init>", "(I)V");  // changed init integer_class
        jobject integer_object = env->NewObject(integer_class, integer_constructor, static_cast<jint>(reply_pair.first));

        derecho::Bytes obj = reply_pair.second.get();
        char* buf = obj.bytes;

        jbyteArray obj_byte_arr = env->NewByteArray(obj.size);

        env->SetByteArrayRegion(obj_byte_arr, 0, obj.size, reinterpret_cast<jbyte*>(buf));

        env->CallObjectMethod(hash_map_object, map_put, integer_object, obj_byte_arr);
    }

    return hash_map_object;
};

/*
 * Class:     io_derecho_QueryResults
 * Method:    getReplyMapP2P
 * Signature: (JI)[B
 */
JNIEXPORT jbyteArray JNICALL Java_io_derecho_QueryResults_getReplyMapP2P(JNIEnv* env, jobject qr_object, jlong handle, jint target_node_id) {
    QueryResultHolder* qrh = reinterpret_cast<QueryResultHolder*>(handle);
    derecho::rpc::QueryResults<derecho::Bytes>* result = qrh->get_result();

    derecho::Bytes obj = result->get().get(static_cast<int>(target_node_id));

    char* buf = obj.bytes;

    jbyteArray obj_byte_arr = env->NewByteArray(obj.size);

    env->SetByteArrayRegion(obj_byte_arr, 0, obj.size, reinterpret_cast<jbyte*>(buf));

    return obj_byte_arr;
};
