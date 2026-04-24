import asyncio

import httpx

from python_webchannel.channel import ChannelState, WebChannel
from python_webchannel.events import Event, EventType, Stat
from python_webchannel.options import WebChannelOptions


class StubAsyncClient:
    def __init__(self):
        self.cookies = httpx.Cookies()
        self.closed = False
        self.posts = []

    async def aclose(self):
        self.closed = True

    async def post(self, *args, **kwargs):
        self.posts.append((args, kwargs))
        return None


def test_injected_client_is_not_closed_by_channel():
    client = StubAsyncClient()
    channel = WebChannel("https://example.com/channel", WebChannelOptions(), http_client=client)

    asyncio.run(channel.close())

    assert client.closed is False


def test_owned_client_is_closed_by_channel():
    channel = WebChannel("https://example.com/channel", WebChannelOptions())
    client = channel._client

    asyncio.run(channel.close())

    assert client.is_closed is True


def test_handshake_payload_opens_channel_and_starts_backchannel():
    client = StubAsyncClient()
    channel = WebChannel("https://example.com/channel", WebChannelOptions(), http_client=client)
    channel._state = ChannelState.OPENING
    channel._last_array_id = 7
    started = []
    opened = []

    channel.listen(EventType.OPEN, lambda payload: opened.append(payload))
    channel._ensure_backchannel = lambda: started.append(True)

    asyncio.run(channel._handle_handshake_payload(["c", "SID123", "hostprefix", 8, 1, 10]))

    assert channel._state == ChannelState.OPENED
    assert channel._sid == "SID123"
    assert channel._host_prefix == "hostprefix"
    assert channel._acknowledged_array_id == 7
    assert started == [True]
    assert opened == [None]


def test_post_response_updates_ack_and_emits_proxy_stat_once():
    client = StubAsyncClient()
    channel = WebChannel(
        "https://example.com/channel",
        WebChannelOptions(detect_buffering_proxy=True),
        http_client=client,
    )
    stats = []
    channel._stats_target.listen(Event.STAT_EVENT, lambda event: stats.append(event.stat))

    asyncio.run(channel._handle_post_response("[1,5,128]"))
    asyncio.run(channel._handle_post_response("[1,6,64]"))

    assert channel._acknowledged_array_id == 6
    assert stats == [Stat.PROXY]


def test_message_dispatch_supports_sync_and_async_listeners():
    client = StubAsyncClient()
    channel = WebChannel("https://example.com/channel", WebChannelOptions(), http_client=client)
    received = []

    async def async_listener(event):
        received.append(("async", event.data))

    def sync_listener(event):
        received.append(("sync", event.data))

    channel.listen(EventType.MESSAGE, sync_listener)
    channel.listen(EventType.MESSAGE, async_listener)

    asyncio.run(channel._handle_channel_payload(["d", {"__data__": '{"hello":"world"}'}]))
    asyncio.run(asyncio.sleep(0))

    assert ("sync", {"hello": "world"}) in received
    assert ("async", {"hello": "world"}) in received
