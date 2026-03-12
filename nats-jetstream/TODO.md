# TODO

## Naming

- Consider renaming `max_wait` to `timeout` on `next()` — it raises `asyncio.TimeoutError` on expiry, so `timeout` is more idiomatic. On `fetch()`/`messages()` `max_wait` maps to the server-side `expires` field and doesn't error, so the naming distinction may be intentional. Decide whether to unify or keep separate names.
