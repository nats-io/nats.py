# Nkeys and User Credentials

In order to use NKEYS and NATS JWTs, first you need to install the NKEYS module
for the NATS Server:

```
pip install nats-py[nkeys]
```

Example of connecting to NATS using JWT creds:

```python
await nats.connect("tls://connect.ngs.global:4222",
                   user_credentials="/path/to/secret.creds")
```
