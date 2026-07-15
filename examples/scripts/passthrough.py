import struct
import time

def on_trigger(trigger: dict, windows: dict) -> list:
    history = windows.get(trigger['stream_id'], [])
    
    # 1. Extract the raw bytes from the payload
    payload_bytes = trigger['payload']
    
    # 2. Unpack the 8 bytes back into a uint64_t (unsigned long long)
    # 'Q' represents an 8-byte unsigned integer. 
    # If Python and C++ are on the same machine, native endianness ('Q') works perfectly.
    try:
        if isinstance(payload_bytes, (bytes, bytearray)):
            payload_ns = struct.unpack("Q", payload_bytes)[0]
        else:
            # Fallback if your framework already converted it to an int/float
            payload_ns = int(payload_bytes)
    except Exception as e:
        print(f"[py] Error parsing payload: {e}")
        payload_ns = 0

    # 3. Get the current system time in nanoseconds
    current_ns = time.time_ns()
    
    # 4. Calculate the time difference (latency)
    latency_ns = current_ns - payload_ns
    latency_ms = latency_ns / 1_000_000.0  # Convert to milliseconds for readability

    # # Print diagnostics
    # print(f"[py] stream={trigger['stream_id']} seq={trigger['sequence']} window={len(history)}")
    # print(f"[py] Payload Time (ns): {payload_ns}")
    # print(f"[py] Current Time (ns): {current_ns}")
    # print(f"[py] Processing Latency: {latency_ms:.3f} ms")
    # print("---")
    
    return [{
        "stream_id": "py_processed",
        "schema_id": "py_v1",
        "payload":   trigger["payload"],
        "sequence":  trigger["sequence"],
    }]