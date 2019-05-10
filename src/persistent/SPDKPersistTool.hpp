#ifndef DERECHO_SPDK_TOOL
#define DERECHO_SPDK_TOOL

/*
 * Print information about all nvme devices on current machine
 */
void get_nvme_device_info();

/*
 * Print information about state of Derecho SPDK persistent layer on specified device
 */
void get_spdk_persistent_info();

/*
 * Format specified NVME device for use with Derecho SPDK persistent layer
 */
void format_spdk_persistent_storage();

#endif
