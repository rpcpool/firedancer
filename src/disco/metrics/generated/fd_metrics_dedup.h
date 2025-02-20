/* THIS FILE IS GENERATED BY gen_metrics.py. DO NOT HAND EDIT. */

#include "../fd_metrics_base.h"
#include "fd_metrics_enums.h"

#define FD_METRICS_COUNTER_DEDUP_TRANSACTION_DEDUP_FAILURE_OFF  (16UL)
#define FD_METRICS_COUNTER_DEDUP_TRANSACTION_DEDUP_FAILURE_NAME "dedup_transaction_dedup_failure"
#define FD_METRICS_COUNTER_DEDUP_TRANSACTION_DEDUP_FAILURE_TYPE (FD_METRICS_TYPE_COUNTER)
#define FD_METRICS_COUNTER_DEDUP_TRANSACTION_DEDUP_FAILURE_DESC "Count of transactions that failed to deduplicate in the dedup stage"
#define FD_METRICS_COUNTER_DEDUP_TRANSACTION_DEDUP_FAILURE_CVT  (FD_METRICS_CONVERTER_NONE)

#define FD_METRICS_COUNTER_DEDUP_GOSSIPED_VOTES_RECEIVED_OFF  (17UL)
#define FD_METRICS_COUNTER_DEDUP_GOSSIPED_VOTES_RECEIVED_NAME "dedup_gossiped_votes_received"
#define FD_METRICS_COUNTER_DEDUP_GOSSIPED_VOTES_RECEIVED_TYPE (FD_METRICS_TYPE_COUNTER)
#define FD_METRICS_COUNTER_DEDUP_GOSSIPED_VOTES_RECEIVED_DESC "Count of simple vote transactions received over gossip instead of via the normal TPU path"
#define FD_METRICS_COUNTER_DEDUP_GOSSIPED_VOTES_RECEIVED_CVT  (FD_METRICS_CONVERTER_NONE)

#define FD_METRICS_DEDUP_TOTAL (2UL)
extern const fd_metrics_meta_t FD_METRICS_DEDUP[FD_METRICS_DEDUP_TOTAL];
