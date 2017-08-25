#ifndef HLC_HPP
#define HLC_HPP
#include <sys/types.h>
#include <inttypes.h>
#include <pthread.h>

class HLC{

private:
  pthread_spinlock_t m_oLck; // spinlock

public:
  uint64_t m_rtc_us; // real-time clock in microseconds
  uint64_t m_logic;  // logic clock

  // constructors
  HLC () noexcept(false);

  HLC (uint64_t _r,uint64_t _l):
    m_rtc_us(_r),m_logic(_l){
  }

  // destructors
  virtual ~HLC() noexcept(false);

  // ticking method - thread safe
  virtual void tick (bool thread_safe=true) noexcept(false);
  virtual void tick (const HLC & msgHlc, bool thread_safe=true) noexcept(false);

  // comparators
  virtual bool operator > (const HLC & hlc) const noexcept(true);
  virtual bool operator < (const HLC & hlc) const noexcept(true);
  virtual bool operator == (const HLC & hlc) const noexcept(true);
  virtual bool operator >= (const HLC & hlc) const noexcept(true);
  virtual bool operator <= (const HLC & hlc) const noexcept(true);

  // evaluator
  virtual void operator = (const HLC & hlc) noexcept(true);
};

#define HLC_EXP(errcode,usercode) \
  ( (((errcode)&0xffffffffull)<<32) | ((usercode)&0xffffffffull) )
#define HLC_EXP_USERCODE(x) ((uint32_t)((x)&0xffffffffull))
#define HLC_EXP_READ_RTC(x)                     HLC_EXP(0,(x))
#define HLC_EXP_SPIN_INIT(x)                    HLC_EXP(1,(x))
#define HLC_EXP_SPIN_DESTROY(x)                 HLC_EXP(2,(x))
#define HLC_EXP_SPIN_LOCK(x)                    HLC_EXP(3,(x))
#define HLC_EXP_SPIN_UNLOCK(x)                    HLC_EXP(4,(x))

// read the rtc clock in microseconds
uint64_t read_rtc_us() noexcept(false);

#endif//HLC_HPP
