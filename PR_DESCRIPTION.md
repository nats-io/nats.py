Closes #868

When a NATS server enters lame duck mode during a rolling restart, it sends `INFO {"ldm": true}` to connected clients. Previously nats.py ignored this field, causing clients to wait until the server forcibly closed the connection before reconnecting -- producing unnecessary errors and downtime.

## Changes

- Detect `ldm: true` in `_process_info` and invoke `lame_duck_mode_cb` callback (matching Go client behavior -- no auto-reconnect, user decides)
- Add `lame_duck_mode_cb` callback option on `connect()` for custom LDM handling
- Add `force_reconnect()` public method for proactive reconnection (can be called from the callback)
- Add `NATSD.send_signal()` test helper

## API

```python
nc = NATS()

async def handle_lame_duck():
    await nc.force_reconnect()

# Custom handler for lame duck mode
await nc.connect(
    servers,
    lame_duck_mode_cb=handle_lame_duck,  # new arg
)

# Force reconnect from anywhere
await nc.force_reconnect()  # new public method
```

## Tests

- [x] `test_lame_duck_callback` -- callback fires on SIGUSR2, no auto-reconnect
- [x] `test_lame_duck_callback_with_force_reconnect` -- callback calls `force_reconnect()`, client moves to other server
- [x] `test_force_reconnect` -- public method works standalone
- [x] Existing test suite passes
