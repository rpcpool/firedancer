#!/usr/bin/env python3
"""Runs test_geyser_tile against grpcurl and checks the stream it serves.

Usage: test_geyser_tile.py path/to/unit-test/test_geyser_tile [--alpenglow] [--port N]

Needs grpcurl on PATH.  The harness exits after its scenario, so grpcurl
ends with an EOF error; only the checks below decide pass or fail.
"""
import base64, hashlib, json, pathlib, subprocess, sys, time

OWNER_A_B58 = 'CVDFLCAjXhVWiPXH9nTCTpCgVzmDVoiPzNJYuccr1dqB'  # 32 bytes of 0xAA

def data_byte(aid, i):
    return ((i * 131 + aid * 17) ^ (i >> 8)) & 0xFF

def expect_data(aid, n):
    return bytes(data_byte(aid, i) for i in range(n))

def parse_stream(text):
    dec, pos, out = json.JSONDecoder(), 0, []
    while True:
        while pos < len(text) and text[pos].isspace():
            pos += 1
        if pos >= len(text) or text[pos] != '{':
            break
        obj, pos = dec.raw_decode(text, pos)
        out.append(obj)
    return out, text[pos:].strip()

def check(text, alpenglow):
    msgs, tail = parse_stream(text)
    slots = [(int(m['slot']['slot']), m['slot'].get('status', 'SLOT_PROCESSED'), m['slot'].get('parent'), m.get('filters')) for m in msgs if 'slot' in m]
    accts = [m for m in msgs if 'account' in m]

    want_slots = [(100, 'SLOT_FIRST_SHRED_RECEIVED', '99'), (100, 'SLOT_COMPLETED', '99'), (100, 'SLOT_CREATED_BANK', '99'),
                  (100, 'SLOT_PROCESSED', '99'), (100, 'SLOT_CONFIRMED', None), (100, 'SLOT_FINALIZED', None), (101, 'SLOT_DEAD', None)]
    got_slots = [(s, st, p) for s, st, p, _ in slots]
    assert got_slots == want_slots, f'slot updates differ:\n got  {got_slots}\n want {want_slots}'
    assert all(f == ['s'] for *_, f in slots), 'slot filter name not echoed'

    # id -> (lamports, data_len, slot); 4, 5, 6 were lost in transit, 8 has another owner
    want = {1: (1001, 100, 100), 2: (1002, 204800, 100), 3: (1003, 10 * 1024 * 1024, 100), 7: (1007, 204800, 101), 9: (0, 0, 101)}
    got_ids, last_wv = [], -1
    for m in accts:
        a = m['account']['account']
        pk = base64.b64decode(a['pubkey'])
        aid = pk[0]
        assert pk == bytes([aid]) * 32, 'pubkey mangled'
        assert aid in want, f'account {aid} must not be delivered'
        lamports, n, slot = want[aid]
        data = base64.b64decode(a.get('data', ''))
        assert len(data) == n, f'account {aid}: data length {len(data)} != {n}'
        assert hashlib.sha256(data).digest() == hashlib.sha256(expect_data(aid, n)).digest(), f'account {aid}: data corrupt'
        assert int(a.get('lamports', 0)) == lamports, f'account {aid}: lamports'
        assert base64.b64decode(a['owner']) == b'\xaa' * 32, f'account {aid}: owner'
        assert base64.b64decode(a['txnSignature']) == bytes([aid]) * 64, f'account {aid}: signature'
        assert int(m['account']['slot']) == slot, f'account {aid}: slot'
        assert m.get('filters') == ['a'], f'account {aid}: filters {m.get("filters")}'
        wv = int(a.get('writeVersion', 0))
        assert wv > last_wv, 'write_version not increasing'
        last_wv = wv
        assert 'createdAt' in m
        got_ids.append(aid)
    assert got_ids == sorted(want), f'accounts delivered {got_ids}, want {sorted(want)}'
    print(f'OK alpenglow={int(alpenglow)}: {len(slots)} slot updates, accounts {got_ids} intact (10 MiB included), lost and filtered accounts absent')
    if tail:
        print('grpcurl tail:', tail[:200])

def main():
    args = sys.argv[1:]
    alpenglow = '--alpenglow' in args
    port = args[args.index('--port') + 1] if '--port' in args else '19998'
    binary = next(a for a in args if not a.startswith('--') and a != port)
    proto = pathlib.Path(__file__).resolve().parent / 'proto'
    harness = subprocess.Popen([binary, '--port', port, '--alpenglow', '1' if alpenglow else '0'],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1)
    request = json.dumps({'slots': {'s': {'interslotUpdates': True}}, 'accounts': {'a': {'owner': [OWNER_A_B58]}}})
    client = subprocess.run(['grpcurl', '-plaintext', '-max-msg-sz', '20000000', '-import-path', str(proto), '-proto', 'geyser.proto',
                             '-d', request, f'127.0.0.1:{port}', 'geyser.Geyser/Subscribe'],
                            capture_output=True, text=True, timeout=120)
    rc = harness.wait(timeout=60)
    assert rc == 0, f'test_geyser_tile exited {rc}'
    check(client.stdout, alpenglow)

main()
