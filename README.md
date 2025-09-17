# python-webchannel

Minimal Python implementation of the Firebase WebChannel protocol. Provides a native Python transport compatible with Firestore's WebChannel-based streaming API.

```python
import asyncio

from python_webchannel import (
    WebChannelOptions,
    create_web_channel_transport,
    EventType,
)


async def main() -> None:
    transport = create_web_channel_transport()
    channel = transport.create_web_channel(
        "https://example.com/google.firestore.v1.Firestore/Listen/channel",
        WebChannelOptions(
            message_url_params={"database": "projects/demo/databases/(default)"},
            http_session_id_param="gsessionid",
            send_raw_json=True,
        ),
    )

    channel.listen(EventType.MESSAGE, lambda event: print("message", event.data))
    await channel.open()
    await channel.send({"example": "payload"})

    # ... use the channel ...

    await channel.close()


if __name__ == "__main__":
    asyncio.run(main())
```

## Status

Early preview. The implementation currently focuses on unidirectional streaming compatible with Firestore. Additional protocol features (multiplexing, origin trials, buffering proxy detection) will be added incrementally.
