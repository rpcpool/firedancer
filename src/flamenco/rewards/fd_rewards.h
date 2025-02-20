#ifndef HEADER_fd_src_flamenco_runtime_program_fd_reward_h
#define HEADER_fd_src_flamenco_runtime_program_fd_reward_h

#include "../fd_flamenco_base.h"
#include "../runtime/context/fd_exec_instr_ctx.h"
#include "../runtime/program/fd_stake_program.h"
#include "../runtime/program/fd_vote_program.h"
#include "../runtime/sysvar/fd_sysvar.h"
#include "../runtime/sysvar/fd_sysvar_epoch_rewards.h"
#include "../runtime/sysvar/fd_sysvar_epoch_schedule.h"
#include "../stakes/fd_stakes.h"

FD_PROTOTYPES_BEGIN

void
fd_update_rewards( fd_exec_slot_ctx_t * slot_ctx,
                   const fd_hash_t *    parent_blockhash,
                   ulong                parent_epoch,
                   fd_epoch_info_t    * temp_info );

void
fd_begin_partitioned_rewards(
                    fd_exec_slot_ctx_t * slot_ctx,
                    const fd_hash_t *    parent_blockhash,
                    ulong                parent_epoch,
                    fd_epoch_info_t    * temp_info );

void
fd_rewards_recalculate_partitioned_rewards(
    fd_exec_slot_ctx_t * slot_ctx
);

void
fd_distribute_partitioned_epoch_rewards( fd_exec_slot_ctx_t * slot_ctx );

FD_PROTOTYPES_END

#endif
