/* test_geyser_tile drives the geyser tile callbacks without a topology.
   It serves gRPC on a local port, waits for one Subscribe, then feeds a
   fixed scenario through the real producer (fd_geyser_acct_publish) and
   the tile's frag callbacks, and exits.  test_geyser_tile.py runs it
   against grpcurl and checks the stream the client received. */

#include "fd_geyser_tile.c"

#if FD_HAS_HOSTED

#include <stdlib.h> /* aligned_alloc */

#define TEST_IN_REPLAY  (0UL)
#define TEST_IN_ACCT    (1UL)
#define TEST_DEPTH      (512UL)
#define TEST_MTU        ((ulong)USHORT_MAX)
#define TEST_DCACHE_SZ  (32UL<<20)

static fd_topo_tile_t   test_tile[1];
static fd_geyser_tile_t test_ctx[1];

/* Producer side of the account link */

static fd_stem_context_t test_stem[1];
static fd_frag_meta_t *  test_mcache[1];
static ulong             test_seqs[1];
static ulong             test_depths[1]   = { TEST_DEPTH };
static ulong             test_cr_avail[1] = { TEST_DEPTH };
static ulong             test_min_cr_avail[1] = { TEST_DEPTH };
static int               test_out_reliable[1];
static uchar *           test_dcache;
static ulong             test_chunk;
static ulong             test_wmark;
static ulong             test_consumed; /* next acct link seq to hand to the tile */

static uchar * test_replay_buf;

static uchar test_owner_a[ 32 ]; /* 32 bytes of 0xAA, the owner the client filters on */
static uchar test_owner_b[ 32 ]; /* 32 bytes of 0xBB */

static uchar
test_data_byte( ulong id,
                ulong i ) {
  return (uchar)( ( i*131UL + id*17UL ) ^ ( i>>8 ) );
}

/* test_acct_publish stages account id onto the account link.  Returns
   the number of fragments published. */

static ulong
test_acct_publish( ulong         id,
                   uchar const * owner,
                   ulong         lamports,
                   ulong         data_len,
                   ulong         slot ) {
  static uchar data[ FD_GEYSER_ACCT_DATA_MAX ];
  for( ulong i=0UL; i<data_len; i++ ) data[ i ] = test_data_byte( id, i );
  uchar pubkey[ 32 ]; fd_memset( pubkey, (int)id, 32UL );
  uchar sig   [ 64 ]; fd_memset( sig,    (int)id, 64UL );
  ulong seq0 = test_seqs[ 0 ];
  fd_geyser_acct_publish( test_stem, 0UL, (fd_wksp_t *)test_dcache, &test_chunk, 0UL, test_wmark, TEST_MTU,
                          pubkey, owner, lamports, 0, data, data_len, slot, sig, 1 );
  return test_seqs[ 0 ]-seq0;
}

/* test_acct_feed hands the next published fragment to the tile, or
   skips it (a fragment the tile lost to an overrun) if !deliver. */

static void
test_acct_feed( int deliver ) {
  FD_TEST( test_consumed<test_seqs[ 0 ] );
  fd_frag_meta_t const * m = test_mcache[ 0 ] + fd_mcache_line_idx( test_consumed, TEST_DEPTH );
  FD_TEST( m->seq==test_consumed );
  if( deliver ) {
    FD_TEST( !before_frag( test_ctx, TEST_IN_ACCT, m->seq, m->sig ) );
    during_frag( test_ctx, TEST_IN_ACCT, m->seq, m->sig, m->chunk, m->sz, m->ctl );
    after_frag ( test_ctx, TEST_IN_ACCT, m->seq, m->sig, m->sz, 0UL, 0UL, NULL );
  }
  test_consumed++;
}

static void
test_acct_feed_all( void ) {
  while( test_consumed<test_seqs[ 0 ] ) test_acct_feed( 1 );
}

static void
test_replay_feed( ulong sig,
                  ulong slot,
                  ulong parent_slot ) {
  ulong sz;
  switch( sig ) {
    case REPLAY_SIG_SLOT_COMPLETED: {
      fd_replay_slot_completed_t * m = fd_type_pun( test_replay_buf );
      m->slot = slot; m->parent_slot = parent_slot; sz = sizeof(*m);
      break;
    }
    case REPLAY_SIG_SLOT_DEAD: {
      fd_replay_slot_dead_t * m = fd_type_pun( test_replay_buf );
      m->slot = slot; sz = sizeof(*m);
      break;
    }
    case REPLAY_SIG_OC_ADVANCED: {
      fd_replay_oc_advanced_t * m = fd_type_pun( test_replay_buf );
      m->slot = slot; sz = sizeof(*m);
      break;
    }
    case REPLAY_SIG_ROOT_ADVANCED: {
      fd_replay_root_advanced_t * m = fd_type_pun( test_replay_buf );
      m->slot = slot; sz = sizeof(*m);
      break;
    }
    default: /* a message the tile must filter */
      FD_TEST( before_frag( test_ctx, TEST_IN_REPLAY, 0UL, sig ) );
      return;
  }
  FD_TEST( !before_frag( test_ctx, TEST_IN_REPLAY, 0UL, sig ) );
  during_frag( test_ctx, TEST_IN_REPLAY, 0UL, sig, 0UL, sz, 0UL );
  after_frag ( test_ctx, TEST_IN_REPLAY, 0UL, sig, sz, 0UL, 0UL, NULL );
}

static void
test_poll( long dt_ns ) {
  long until = fd_log_wallclock() + dt_ns;
  do {
    int charge_busy = 0;
    fd_grpc_server_poll( test_ctx->server, &charge_busy );
    if( !charge_busy ) fd_log_sleep( (long)100e3 );
  } while( fd_log_wallclock()<until );
}

static int
test_has_subscriber( void ) {
  for( ulong i=0UL; i<test_ctx->max_conn_cnt; i++ ) {
    if( fd_grpc_server_has_stream( test_ctx->server, i ) && test_ctx->subs[ i ].wants_slots && test_ctx->subs[ i ].acct_filter_cnt ) return 1;
  }
  return 0;
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  ushort port      = fd_env_strip_cmdline_ushort( &argc, &argv, "--port",      NULL, (ushort)19998 );
  int    alpenglow = fd_env_strip_cmdline_int   ( &argc, &argv, "--alpenglow", NULL, 0             );

  fd_memset( test_owner_a, 0xAA, 32UL );
  fd_memset( test_owner_b, 0xBB, 32UL );

  test_tile->in_cnt                     = 2UL;
  test_tile->geyser.listen_addr         = FD_IP4_ADDR( 127,0,0,1 );
  test_tile->geyser.listen_port         = port;
  test_tile->geyser.max_connections     = 4UL;
  test_tile->geyser.send_buffer_size_mb = 64UL;
  test_tile->geyser.alpenglow           = alpenglow;

  /* Tile context, laid out by hand in place of the tile scratch */

  fd_geyser_tile_t *      ctx    = test_ctx;
  fd_grpc_server_params_t params = derive_server_params( test_tile );
  ctx->subs         = calloc( test_tile->geyser.max_connections, sizeof(geyser_sub_t) );
  ctx->acct_enc_buf = aligned_alloc( 16UL, fd_ulong_align_up( GEYSER_ACCT_ENC_BUF_SZ, 16UL ) );
  ctx->reasm_pool   = aligned_alloc( 16UL, test_tile->in_cnt*FD_GEYSER_ACCT_DATA_MAX );
  ctx->max_conn_cnt = test_tile->geyser.max_connections;
  ctx->alpenglow    = alpenglow;
  void * server_mem = aligned_alloc( fd_grpc_server_align(), fd_grpc_server_footprint( params ) );
  FD_TEST( ctx->subs && ctx->acct_enc_buf && ctx->reasm_pool && server_mem );
  ctx->server = fd_grpc_server_join( fd_grpc_server_new( server_mem, params, &geyser_grpc_callbacks, ctx ) );
  FD_TEST( ctx->server );
  FD_TEST( fd_grpc_server_listen( ctx->server, test_tile->geyser.listen_addr, port )>=0 );

  test_replay_buf = aligned_alloc( FD_CHUNK_ALIGN, fd_ulong_align_up( sizeof(fd_replay_message_t), FD_CHUNK_ALIGN ) );
  test_dcache     = aligned_alloc( FD_CHUNK_ALIGN, TEST_DCACHE_SZ );
  FD_TEST( test_replay_buf && test_dcache );
  fd_memset( test_replay_buf, 0, sizeof(fd_replay_message_t) );
  test_wmark = ( TEST_DCACHE_SZ - fd_ulong_align_up( TEST_MTU, FD_CHUNK_ALIGN ) )>>FD_CHUNK_LG_SZ;

  ctx->in_kind[ TEST_IN_REPLAY ] = IN_KIND_REPLAY;
  ctx->in[ TEST_IN_REPLAY ].mem    = (fd_wksp_t *)test_replay_buf;
  ctx->in[ TEST_IN_REPLAY ].chunk0 = 0UL;
  ctx->in[ TEST_IN_REPLAY ].wmark  = 0UL;
  ctx->in[ TEST_IN_REPLAY ].mtu    = sizeof(fd_replay_message_t);
  ctx->in_kind[ TEST_IN_ACCT ]   = IN_KIND_ACCT;
  ctx->in[ TEST_IN_ACCT ].mem      = (fd_wksp_t *)test_dcache;
  ctx->in[ TEST_IN_ACCT ].chunk0   = 0UL;
  ctx->in[ TEST_IN_ACCT ].wmark    = test_wmark;
  ctx->in[ TEST_IN_ACCT ].mtu      = TEST_MTU;

  void * mcache_mem = aligned_alloc( FD_MCACHE_ALIGN, fd_mcache_footprint( TEST_DEPTH, 0UL ) );
  FD_TEST( mcache_mem );
  test_mcache[ 0 ] = fd_mcache_join( fd_mcache_new( mcache_mem, TEST_DEPTH, 0UL, 0UL ) );
  FD_TEST( test_mcache[ 0 ] );
  test_stem->mcaches             = test_mcache;
  test_stem->seqs                = test_seqs;
  test_stem->depths              = test_depths;
  test_stem->cr_avail            = test_cr_avail;
  test_stem->min_cr_avail        = test_min_cr_avail;
  test_stem->cr_decrement_amount = 1UL;
  test_stem->out_reliable        = test_out_reliable; /* 0: unreliable, as wired in the topology */

  FD_LOG_NOTICE(( "test geyser tile listening at 127.0.0.1:%hu, waiting for a subscriber", port ));
  long deadline = fd_log_wallclock() + (long)30e9;
  while( !test_has_subscriber() ) {
    test_poll( (long)1e6 );
    if( FD_UNLIKELY( fd_log_wallclock()>deadline ) ) FD_LOG_ERR(( "no subscriber with a slots and an accounts filter within 30s" ));
  }
  test_poll( (long)50e6 );

  geyser_reasm_t const * r = &ctx->reasm[ TEST_IN_ACCT ];

  /* Slot 100 completes: FIRST_SHRED_RECEIVED, COMPLETED, CREATED_BANK,
     PROCESSED, each with parent 99.  A filtered message adds nothing. */
  test_replay_feed( REPLAY_SIG_TXN_EXECUTED,   100UL,  0UL );
  test_replay_feed( REPLAY_SIG_SLOT_COMPLETED, 100UL, 99UL );

  /* Account 1: one fragment.  Account 2: 200 KiB, 4 fragments.
     Account 3: the 10 MiB maximum.  All delivered intact. */
  FD_TEST( test_acct_publish( 1UL, test_owner_a, 1001UL, 100UL,                   100UL )==1UL ); test_acct_feed_all();
  FD_TEST( test_acct_publish( 2UL, test_owner_a, 1002UL, 204800UL,                100UL )==4UL ); test_acct_feed_all();
  test_poll( (long)200e6 );
  FD_TEST( test_acct_publish( 3UL, test_owner_a, 1003UL, FD_GEYSER_ACCT_DATA_MAX, 100UL )>100UL ); test_acct_feed_all();
  FD_TEST( !r->active );
  test_poll( (long)2e9 );

  /* Account 4 loses a middle fragment: not delivered. */
  FD_TEST( test_acct_publish( 4UL, test_owner_a, 1004UL, 204800UL, 100UL )==4UL );
  test_acct_feed( 1 ); test_acct_feed( 1 ); test_acct_feed( 0 ); test_acct_feed( 1 );
  FD_TEST( !r->active );

  /* Accounts 5 and 6 have the same size.  The tile sees the head of 5
     and the tail of 6.  The byte count matches account 5, but the
     sequence gap must stop the tile from splicing them: neither is
     delivered. */
  FD_TEST( test_acct_publish( 5UL, test_owner_a, 1005UL, 204800UL, 100UL )==4UL );
  FD_TEST( test_acct_publish( 6UL, test_owner_a, 1006UL, 204800UL, 100UL )==4UL );
  test_acct_feed( 1 ); test_acct_feed( 1 ); test_acct_feed( 0 ); test_acct_feed( 0 );
  test_acct_feed( 0 ); test_acct_feed( 0 ); test_acct_feed( 1 ); test_acct_feed( 1 );
  FD_TEST( !r->active );

  /* Account 7 follows the losses and is delivered intact.  Account 8
     has another owner and is filtered.  Account 9 is closed: zero
     lamports and no data, delivered. */
  FD_TEST( test_acct_publish( 7UL, test_owner_a, 1007UL, 204800UL, 101UL )==4UL ); test_acct_feed_all();
  FD_TEST( test_acct_publish( 8UL, test_owner_b, 1008UL, 100UL,    101UL )==1UL ); test_acct_feed_all();
  FD_TEST( test_acct_publish( 9UL, test_owner_a, 0UL,    0UL,      101UL )==1UL ); test_acct_feed_all();
  FD_TEST( !r->active );

  /* Tower: OC_ADVANCED gives CONFIRMED and ROOT_ADVANCED gives
     FINALIZED.  Alpenglow: there is no OC_ADVANCED, and ROOT_ADVANCED
     gives CONFIRMED then FINALIZED.  Slot 101 is dead. */
  if( !alpenglow ) test_replay_feed( REPLAY_SIG_OC_ADVANCED,   100UL, 0UL );
  /**/             test_replay_feed( REPLAY_SIG_ROOT_ADVANCED, 100UL, 0UL );
  /**/             test_replay_feed( REPLAY_SIG_SLOT_DEAD,     101UL, 0UL );

  test_poll( (long)2e9 );
  FD_TEST( test_has_subscriber() ); /* the subscriber kept up and was not evicted */

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}

#else

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  FD_LOG_WARNING(( "skip: unit test requires FD_HAS_HOSTED" ));
  fd_halt();
  return 0;
}

#endif
