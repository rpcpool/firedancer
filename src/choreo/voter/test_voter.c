#include "../../ballet/ed25519/fd_ed25519.h"
#include "../../choreo/fd_choreo.h"
#include "../../flamenco/fd_flamenco.h"
#include "../../flamenco/runtime/fd_system_ids.h"
#include "../../flamenco/txn/fd_txn_generate.h"
#include "../../util/fd_util.h"
#include <sys/random.h>

#define TEST_VOTE_TXN_MAGIC ( 0x7e58UL )

void
vote_txn_signer( void * keypair, uchar signature[static 64], uchar const * buffer, ulong len ) {
  fd_sha512_t sha;
  uchar *     validator_identity_keypair = (uchar *)fd_type_pun( keypair );
  fd_ed25519_sign( /* sig */ signature,
                   /* msg */ buffer,
                   /* sz  */ len,
                   /* public_key  */ validator_identity_keypair + 32UL,
                   /* private_key */ validator_identity_keypair,
                   &sha );
}

int
main( int argc, char ** argv ) {
  fd_boot( &argc, &argv );
  fd_flamenco_boot( &argc, &argv );

  /* keys */
  uchar       validator_identity_keypair[64];
  fd_pubkey_t vote_acct_addr;
  FD_TEST( 64UL == getrandom( validator_identity_keypair, 64UL, 0 ) );
  FD_TEST( 32UL == getrandom( vote_acct_addr.key, 32UL, 0 ) );
  FD_PARAM_UNUSED fd_pubkey_t * validator_identity = (fd_pubkey_t *)fd_type_pun_const( validator_identity_keypair +
                                                                       32 );

  /* workspace */
  ulong       page_cnt = 1;
  char *      _page_sz = "gigantic";
  ulong       numa_idx = fd_shmem_numa_idx( 0 );
  fd_wksp_t * wksp     = fd_wksp_new_anonymous( fd_cstr_to_shmem_page_sz( _page_sz ),
                                            page_cnt,
                                            fd_shmem_cpu_idx( numa_idx ),
                                            "wksp",
                                            0UL );
  FD_TEST( wksp );

  /* alloc */
  void *                       alloc_shmem   = fd_wksp_alloc_laddr( wksp,
                                            fd_alloc_align(),
                                            fd_alloc_footprint(),
                                            TEST_VOTE_TXN_MAGIC );
  void *                       alloc_shalloc = fd_alloc_new( alloc_shmem, TEST_VOTE_TXN_MAGIC );
  FD_PARAM_UNUSED fd_alloc_t * alloc         = fd_alloc_join( alloc_shalloc, 0UL );
  FD_PARAM_UNUSED fd_valloc_t  valloc        = fd_alloc_virtual( alloc );

  /* create compact_vote_state_update with dummy values */
  fd_compact_vote_state_update_t compact_vote_update = {0};
  compact_vote_update.root         = 100;
  compact_vote_update.lockouts_len = 0;
  static long now                  = 1715701506716580798L;
  compact_vote_update.has_timestamp = 1;
  compact_vote_update.timestamp     = now;
  FD_TEST( 32UL == getrandom( compact_vote_update.hash.key, 32UL, 0 ) );

  fd_hash_t recent_blockhash = { 0 };
  FD_TEST( 32UL == getrandom( &recent_blockhash.uc, 32UL, 0 ) );

  /* create the vote transaction */
  // uchar txn_meta_buf[FD_TXN_MAX_SZ];
  // uchar txn_buf[FD_TXN_MTU];

  // /* parse the transaction back */
  // ushort                         parsed_recent_blockhash_off;
  // fd_compact_vote_state_update_t parsed_vote_update;
  // FD_TEST( FD_VOTER_OK == fd_vote_txn_parse( txn_buf,
  //                                            txn_size,
  //                                            valloc,
  //                                            &parsed_recent_blockhash_off,
  //                                            &parsed_vote_update ) );
  // FD_LOG_NOTICE(( "recent blockhash: %s == %s",
  //                 FD_BASE58_ENC_32_ALLOCA( recent_blockhash ),
  //                 FD_BASE58_ENC_32_ALLOCA( txn_buf + parsed_recent_blockhash_off ) ));
  // FD_LOG_NOTICE(( "root: %ld == %ld", compact_vote_update.root, parsed_vote_update.root ) );
  // FD_LOG_NOTICE(( "timestamp: %lu == %lu",
  //                 *compact_vote_update.timestamp,
  //                 *parsed_vote_update.timestamp ) );
  // FD_LOG_NOTICE(( "hash: %s == %s",
  //                 FD_BASE58_ENC_32_ALLOCA( compact_vote_update.hash.key ),
  //                 FD_BASE58_ENC_32_ALLOCA( parsed_vote_update.hash.key ) ));
}
