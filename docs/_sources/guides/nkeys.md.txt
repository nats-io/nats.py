Nkeys and User Credentials (NATS v2)
-------------------------------------

In order to use nkeys and NATS JWTs, first you need to install the nkeys module
for the NATS Server:


```
pip install nats-py[nkeys]
```

Example of connecting to NATS using JWT creds:

```python
await nats.connect("tls://connect.ngs.global:4222",
                   user_credentials="/path/to/secret.creds")
```
