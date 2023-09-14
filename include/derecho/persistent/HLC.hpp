#ifndef HLC_HPP
#define HLC_HPP
#include <inttypes.h>
#include <pthread.h>
#include <sys/types.h>

/**
 * @brief Hybrid Logical clock
 */
class HLC {
private:
    /**
     * The Hybrid clock
     */
    pthread_spinlock_t m_oLck;  // spinlock

public:
    /**
     * The real-time clock component (in microseconds)
     */
    uint64_t m_rtc_us;
    /**
     * The logic clock component
     */
    uint64_t m_logic;

    /**
     * Default constructor
     */
    HLC();

    /**
     * Constructor
     */
    HLC(uint64_t _r, uint64_t _l);

    /**
     * Destructor
     */
    virtual ~HLC() noexcept(false);

    /**
     * Local tick
     */
    virtual void tick(bool thread_safe = true);

    /**
     * Tick with incoming message
     */
    virtual void tick(const HLC& msgHlc, bool thread_safe = true);

    /**
     * Comparators
     */
    virtual bool operator>(const HLC& hlc) const noexcept(true);
    virtual bool operator<(const HLC& hlc) const noexcept(true);
    virtual bool operator==(const HLC& hlc) const noexcept(true);
    virtual bool operator>=(const HLC& hlc) const noexcept(true);
    virtual bool operator<=(const HLC& hlc) const noexcept(true);

    /**
     * Evaluator
     */
    virtual void operator=(const HLC& hlc) noexcept(true);
};

/**
 * @cond DoxygenSuppressed
 */
#define HLC_EXP(errcode, usercode) \
    ((((errcode)&0xffffffffull) << 32) | ((usercode)&0xffffffffull))
#define HLC_EXP_USERCODE(x) ((uint32_t)((x)&0xffffffffull))
#define HLC_EXP_READ_RTC(x) HLC_EXP(0, (x))
#define HLC_EXP_SPIN_INIT(x) HLC_EXP(1, (x))
#define HLC_EXP_SPIN_DESTROY(x) HLC_EXP(2, (x))
#define HLC_EXP_SPIN_LOCK(x) HLC_EXP(3, (x))
#define HLC_EXP_SPIN_UNLOCK(x) HLC_EXP(4, (x))

// read the rtc clock in microseconds
uint64_t read_rtc_us();

/**
 * @endcond
 */
#endif  //HLC_HPP
