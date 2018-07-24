# !/usr/bin/env python3
# encoding=utf-8

from collections import deque, namedtuple
from bencode import bencode, bdecode, BTFailure
from socket import inet_ntoa
from struct import unpack
from os import urandom
from functools import partial
import socket
import asyncio
import aioredis
import codecs


def random_nid():
    return urandom(20)


def random_tid():
    return urandom(2)


def fake_nid(self_nid, other_nid, sep=2):
    return other_nid[:-sep] + self_nid[-sep:]


hex_encode = codecs.getencoder('hex')


def encode_infohash(hashinfo):
    return hex_encode(hashinfo)[0].decode()


def split_nodes(nodes):
    length = len(nodes)
    if length % 26 != 0:
        return
    for i in range(0, len(nodes), 26):
        nid = nodes[i:i + 20]
        ip = inet_ntoa(nodes[i + 20:i + 24])
        port = unpack('!H', nodes[i + 24:i + 26])[0]
        yield nid, ip, port


Node = namedtuple('Node', 'nid,ip,port')

TRACKERS = [
    ('router.bittorrent.com', 6881),
    ('dht.transmissionbt.com', 6881),
    ('router.utorrent.com', 6881),
]

REDIS_KEY = 'btih'


class DHT(asyncio.DatagramProtocol):
    def __init__(self, port=1024, ip='0.0.0.0', max_node_qsize=2000, limit_get_peers=True, loop=None):
        self.port = port
        self.ip = ip
        self.nid = random_nid()
        self.tid = random_tid()
        self.loop = loop or asyncio.get_event_loop()
        self.transport = None
        self.redis = None
        self.running = False
        self.nodes = deque(maxlen=max_node_qsize)
        self.interval = 1 / max_node_qsize
        self.limit = limit_get_peers
        self.fake_nid = partial(fake_nid, self.nid)
        self.ban_fn_reply = False

    def bootstrap(self):
        for address in TRACKERS:
            self.send_find_node(address)

    def send_find_node(self, address, nid=None):
        nid = self.fake_nid(nid) if nid else self.nid
        tid = self.tid
        msg = dict(
            t=tid,
            y='q',
            q='find_node',
            a=dict(
                id=nid,
                target=random_nid()
            )
        )
        self.send_krpc(msg, address)

    def send_krpc(self, msg, address):
        if not self.transport:
            return
        try:
            self.transport.sendto(bencode(msg), address)
        except OSError:
            pass

    def on_message(self, msg, address):
        msg_type = msg.get('y', b'e')
        if type == b'e':
            return
        if msg_type == b'r':
            if 'nodes' in msg['r']:
                asyncio.ensure_future(self.on_find_node_reply(msg), loop=self.loop)
        elif msg_type == b'q':
            if msg['q'] == b'get_peers':
                self.on_get_peers(msg, address)
            elif msg['q'] == b'announce_peer':
                self.on_announce_peer(msg, address)
            elif msg['q'] == b'find_node':
                self.on_find_node_query(msg, address)
            elif msg['q'] == b'ping':
                self.on_ping(msg, address)

    def on_ping(self, msg, address):
        tid = msg['t']
        nid = msg['a']['id']
        ret = dict(
            t=tid,
            y='r',
            r=dict(id=self.fake_nid(nid))
        )
        self.send_krpc(ret, address)

    def on_find_node_query(self, msg, address):
        nid = msg['a']['id']
        tid = msg['t']
        res = dict(
            t=tid,
            y='r',
            r=dict(id=self.fake_nid(nid), nodes='')
        )
        self.send_krpc(res, address)

    async def on_find_node_reply(self, msg):
        if len(self.nodes) == self.nodes.maxlen:
            self.ban_fn_reply = True
            return
        if len(self.nodes) < self.nodes.maxlen:
            self.ban_fn_reply = False
        if self.ban_fn_reply:
            return
        nodes = split_nodes(msg['r']['nodes'])
        for node in nodes:
            nid, ip, port = node
            if len(nid) != 20 or ip == self.ip or port < 1 or port > 65536:
                continue
            n = Node(nid, ip, port)
            self.nodes.append(n)
            await asyncio.sleep(self.interval)

    def on_get_peers(self, msg, address):
        try:
            infohash = msg['a']['info_hash']
            tid = msg['t']
            nid = msg['a']['id']
            token = infohash[:4]
            res = dict(
                t=tid,
                y='r',
                r=dict(
                    id=self.fake_nid(nid),
                    nodes='',
                    token=token
                )
            )
            self.send_krpc(res, address)
            if not self.limit:
                self.on_get_infohash(infohash)
        except KeyError:
            pass

    def on_announce_peer(self, msg, address):
        try:
            tid = msg['t']
            nid = msg['a']['id']
            res = dict(
                t=tid,
                y='r',
                r=dict(id=self.fake_nid(nid))
            )
            self.send_krpc(res, address)
            a = msg['a']
            infohash = a['info_hash']
            token = a['token']
            if infohash[:4] == token:
                self.on_get_infohash(infohash)
        except KeyError:
            pass

    def on_get_infohash(self, infohash):
        asyncio.ensure_future(self.save_magnet(encode_infohash(infohash)), loop=self.loop)

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, address):
        try:
            msg = bdecode(data)
            self.on_message(msg, address)
        except BTFailure:
            pass

    async def wait_reply(self):
        time = 0
        while len(self.nodes) == 0:
            await asyncio.sleep(self.interval)
            time += self.interval
            if time > 5:
                self.bootstrap()

    async def connect_redis(self):
        try:
            self.redis = await aioredis.create_redis_pool('redis://localhost', minsize=10, maxsize=100, loop=self.loop)
        except ConnectionRefusedError:
            print('连接Reids失败')
            self.stop()

    async def stop_redis(self):
        if self.redis is None:
            return
        self.redis.close()
        await self.redis.wait_closed()

    async def find_node_loop(self):
        self.bootstrap()
        self.running = True
        while self.running:
            try:
                node = self.nodes.popleft()
                self.send_find_node((node.ip, node.port), node.nid)
                await asyncio.sleep(self.interval)
            except IndexError:
                await self.wait_reply()

    async def save_magnet(self, infohash):
        await self.redis.zincrby(REDIS_KEY, 1, infohash)

    def stop(self):
        self.running = False
        for t in asyncio.Task.all_tasks(loop=self.loop):
            t.cancel()
        self.loop.run_until_complete(asyncio.gather(self.stop_redis(), self.loop.shutdown_asyncgens()))
        if self.transport:
            self.transport.close()
        self.loop.stop()

    def start_server(self):
        listen = self.loop.create_datagram_endpoint(lambda: self, local_addr=(self.ip, self.port))
        task_listen = asyncio.ensure_future(listen, loop=self.loop)
        self.loop.run_until_complete(asyncio.gather(task_listen, self.connect_redis()))
        self.transport, _ = task_listen.result()
        self.bootstrap()
        asyncio.ensure_future(self.find_node_loop(), loop=self.loop)
        self.loop.run_forever()

    def start(self):
        try:
            self.start_server()
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    dht = DHT(port=8426)
    dht.start()
