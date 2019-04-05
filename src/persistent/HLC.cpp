#include "HLC.hpp"
#include <errno.h>
#include <time.h>

// return microsecond
uint64_t read_rtc_us() noexcept(false) {
    struct timespec tp;
    if(clock_gettime(CLOCK_REALTIME, &tp) != 0) {
        throw HLC_EXP_READ_RTC(errno);
    } else {
        return (uint64_t)tp.tv_sec * 1000000 + tp.tv_nsec / 1000;
    }
}

HLC::HLC() noexcept(false) {
    this->m_rtc_us = read_rtc_us();
    this->m_logic = 0L;
    if(pthread_spin_init(&this->m_oLck, PTHREAD_PROCESS_SHARED) != 0) {
        throw HLC_EXP_SPIN_INIT(errno);
    }
}

HLC::HLC(uint64_t _r, uint64_t _l) : m_rtc_us(_r), m_logic(_l) {
    if(pthread_spin_init(&this->m_oLck, PTHREAD_PROCESS_SHARED) != 0) {
        throw HLC_EXP_SPIN_INIT(errno);
    }
}

HLC::~HLC() noexcept(false) {
    if(pthread_spin_destroy(&this->m_oLck) != 0) {
        throw HLC_EXP_SPIN_DESTROY(errno);
    }
}

#define HLC_LOCK                                \
    if(pthread_spin_lock(&this->m_oLck) != 0) { \
        throw HLC_EXP_SPIN_LOCK(errno);         \
    }
#define HLC_UNLOCK                                \
    if(pthread_spin_unlock(&this->m_oLck) != 0) { \
        throw HLC_EXP_SPIN_UNLOCK(errno);         \
    }

void HLC::tick(bool thread_safe) noexcept(false) {
    if(thread_safe) {
        HLC_LOCK
    }

    uint64_t rtc = read_rtc_us();
    if(rtc <= this->m_rtc_us) {
        this->m_logic++;
    } else {
        this->m_rtc_us = rtc;
        this->m_logic = 0ull;
    }

    if(thread_safe) {
        HLC_UNLOCK
    }
}

void HLC::tick(const HLC &msgHlc, bool thread_safe) noexcept(false) {
    if(thread_safe) {
        HLC_LOCK
    }

    uint64_t rtc = read_rtc_us();
    if((rtc > this->m_rtc_us) && (rtc > msgHlc.m_rtc_us)) {
        // use rtc
        this->m_rtc_us = rtc;
        this->m_logic = 0ull;
    } else if(*this >= msgHlc) {
        // use this hlc
        this->m_logic++;
    } else {
        // use msg hlc
        this->m_rtc_us = msgHlc.m_rtc_us;
        this->m_logic = msgHlc.m_logic + 1;
    }

    if(thread_safe) {
        HLC_UNLOCK
    }
}

bool HLC::operator>(const HLC &hlc) const
        noexcept(true) {
    return (this->m_rtc_us > hlc.m_rtc_us) || (this->m_rtc_us == hlc.m_rtc_us && this->m_logic > hlc.m_logic);
}

bool HLC::operator<(const HLC &hlc) const
        noexcept(true) {
    return hlc > *this && !(hlc == *this);
}

bool HLC::operator==(const HLC &hlc) const
        noexcept(true) {
    return this->m_rtc_us == hlc.m_rtc_us && this->m_logic == hlc.m_logic;
}

bool HLC::operator>=(const HLC &hlc) const
        noexcept(true) {
    return !(hlc > *this);
}

bool HLC::operator<=(const HLC &hlc) const
        noexcept(true) {
    return !(*this > hlc);
}

void HLC::operator=(const HLC &hlc) noexcept(true) {
    this->m_rtc_us = hlc.m_rtc_us;
    this->m_logic = hlc.m_logic;
}
