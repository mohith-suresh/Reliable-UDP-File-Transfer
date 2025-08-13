# Fast, Reliable UDP File Transfer (Fresh Implementation)

This is a clean-room implementation of a UDP-based, reliable file transfer tool
designed for EE 542 Lab 2. It uses a simple custom protocol (INFO/DATA/NACK/FIN)
with batched negative acknowledgements (NACK) for efficient recovery over
lossy/high-latency links.

## Build

```
make
```

Generates two binaries: `ft_client` and `ft_server`.

## Run

Client sends a file to the server:

```
./ft_server <output_file_path> <listen_port>
./ft_client <input_file_path> <server_ip> <server_port>
```

- The client timestamps from first INFO send to final FIN-ACK receive and prints
  total time and throughput (Mbit/s).
- The server writes the received file to the specified output path.

Environment assumptions: Linux, UDP allowed, 3-node setup (client, VyOS router,
server). Test with MTU 1500 and 9000/9001 consistently across client, router,
and server.

### CLI Options and Tuning

- `ft_client`:
  - `--chunk N`: application payload bytes per DATA message (default 1460; max 65535). Use 1460 for MTU=1500; ~8960 for MTU≈9000.
  - `--rate-mbit X`: request kernel pacing at X Mbit/s (Linux only, best-effort via `SO_MAX_PACING_RATE`). Use with `fq` qdisc (below).
  - `--batch N`: batch N DATA messages per syscall using `sendmmsg` (Linux). Reduces syscall overhead; try 16–64.
  - `--mmap`: memory-map the input file (avoids an extra user-space copy before send).
  - `--zerocopy`: Linux only. Enables `SO_ZEROCOPY` and uses zero-copy send path with batching. Requires a reasonably new kernel/NIC; falls back automatically if unsupported.
  - `--verbose`: print chosen chunk size and pacing; detailed timing.

- `ft_server`:
  - `--verbose`: log retransmission rounds and completion status.

Examples
```
# MTU 1500
./ft_server --verbose /tmp/out.bin 5000
./ft_client --verbose --chunk 1460 --batch 32 /tmp/data.bin <server_ip> 5000

# MTU 9000/9001 (jumbo)
./ft_server --verbose /tmp/out.bin 5000
./ft_client --verbose --chunk 8960 --batch 32 /tmp/data.bin <server_ip> 5000

# Optional kernel pacing to ~100 Mbit/s (Linux only)
sudo tc qdisc add dev <iface> root fq
./ft_client --rate-mbit 100 --chunk 1460 /tmp/data.bin <server_ip> 5000

# Optional mmap + zero-copy (Linux only, advanced)
./ft_client --mmap --zerocopy --chunk 1460 --batch 32 /tmp/data.bin <server_ip> 5000
```

Notes
- For best performance on high RTT/loss paths, prefer larger chunks with jumbo MTU to reduce syscalls/overhead.
- If `SO_MAX_PACING_RATE` is unsupported, `--rate-mbit` is ignored; you can still rely on the app’s light pacing.
- `--zerocopy` uses Linux `SO_ZEROCOPY` to reduce kernel data copies. It requires recent kernels and compatible NICs. If unavailable, the client automatically falls back to the normal path.
`


## Protocol Summary (High-Level)

- INFO: client -> server with file size, chunk size, count, session id; server replies IACK
- DATA: client -> server chunks with sequence numbers
- P1DN: client -> server indicates end of first pass
- NACK: server -> client with a batch of missing sequence ids (variable size)
- DONE: server -> client indicates no more missing chunks
- FIN / FACK: finalize and close

The server receives concurrently while it computes and sends NACK batches. Client
resends requested chunks until DONE, then exchanges FIN/FACK and exits.
