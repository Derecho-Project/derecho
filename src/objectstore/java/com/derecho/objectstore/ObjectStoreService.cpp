#include <derecho/objectstore/ObjectStore.hpp>
#include "com_derecho_objectstore_ObjectStoreService.h"
#include <derecho/conf/conf.hpp>
#include <iostream>
#include <sstream>
#include <string.h>
void throwJavaException(JNIEnv* env, const char* msg) {
    jclass Exception = env->FindClass("java/lang/Exception");
    env->ThrowNew(Exception, msg);
}
int argc;
char** argv;

JNIEXPORT void JNICALL
Java_com_derecho_objectstore_ObjectStoreService_put(JNIEnv* env, jobject obj, jlong oid, jstring jdata) {
    const char* data = env->GetStringUTFChars(jdata, NULL);
    objectstore::Object object(oid, data, strlen(data) + 1);

    try {
        auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv,
                                                                            [&](const objectstore::OID& oid, const objectstore::Object& object) {
                                                                                std::cout << "watcher: " << oid << "->" << object << std::endl;
                                                                            });
        oss.bio_put(object);
    } catch(std::exception& ex) {
        throwJavaException(env, ex.what());
    } catch(...) {
        throwJavaException(env, "Caught unknown exception in put.");
    }
}

JNIEXPORT jboolean JNICALL Java_com_derecho_objectstore_ObjectStoreService_remove(JNIEnv* env, jobject obj, jlong oid) {
    auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv,
                                                                        [&](const objectstore::OID& oid, const objectstore::Object& object) {
                                                                            std::cout << "watcher: " << oid << "->" << object << std::endl;
                                                                        });
    try {
        oss.bio_remove(oid);
        return true;
    } catch(...) {
        std::cout << "error in remove" << std::endl;
        return false;
    }
}

JNIEXPORT jstring JNICALL Java_com_derecho_objectstore_ObjectStoreService_get(JNIEnv* env, jobject obj, jlong oid) {
    try {
        auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv,
                                                                            [&](const objectstore::OID& oid, const objectstore::Object& object) {
                                                                                std::cout << "watcher: " << oid << "->" << object << std::endl;
                                                                            });
        objectstore::Object obj = oss.bio_get(oid);
        std::cout << obj;
        std::stringstream ss;
        ss << std::cout.rdbuf();
        std::string str = ss.str();
        return env->NewStringUTF(str.c_str());
    } catch(...) {
        return NULL;
    }
}

JNIEXPORT void JNICALL Java_com_derecho_objectstore_ObjectStoreService_leave(JNIEnv* env, jobject obj) {
    try {
        auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv,
                                                                            [&](const objectstore::OID& oid, const objectstore::Object& object) {
                                                                                std::cout << "watcher: " << oid << "->" << object << std::endl;
                                                                            });
        oss.leave();
    } catch(...) {
        throwJavaException(env, "Caught unknown exception in remove.");
    }
}

JNIEXPORT void JNICALL Java_com_derecho_objectstore_ObjectStoreService_initialize(JNIEnv* env, jobject obj, jstring jargv) {
    const char* argv_str = env->GetStringUTFChars(jargv, NULL);

    // parsing argv
    char* argv_copy = strdup(argv_str);
    char* tokens[50];
    int n = 0;

    for(char* p = strtok(argv_copy, " "); p; p = strtok(NULL, " ")) {
        if(n >= 50) {
            // maximum number of storable tokens exceeded
            break;
        }
        tokens[n++] = p;
    }

    argc = n;
    argv = tokens;
    try {
        derecho::Conf::initialize(argc, argv);

    } catch(std::exception& ex) {
        throwJavaException(env, ex.what());
    } catch(...) {
        throwJavaException(env, "Caught unknown exception in init.");
    }
}
