from collections import deque, namedtuple
from bencode import bencode, bdecode, BTFailure
from socket import inet_ntoa
from struct import unpack
from os import urandom
from functools import partial
import socket
import asyncio
import codecs
import aioredis
import time


def random_nid():
    return urandom(20)


def random_tid():
    return urandom(2)


def fake_nid(self_nid, other_nid, sep=2):
    return other_nid[:-sep] + self_nid[-sep:]


def encode_infohash(infohash):
    return codecs.getencoder('hex')(infohash)[0].decode()


def split_nodes(nodes):
    length = len(nodes)
    if length % 26 != 0:
        return
    for i in range(0, len(nodes), 26):
        nid = nodes[i:i + 20]
        ip = inet_ntoa(nodes[i + 20:i + 24])
        port = unpack('!H', nodes[i + 24:i + 26])[0]
        yield (nid, ip, port)


now = lambda: time.time()

Node = namedtuple('Node', 'nid,ip,port')

TRACKERS = [
    ('router.bittorrent.com', 6881),
    ('dht.transmissionbt.com', 6881),
    ('router.utorrent.com', 6881),
]

REDIS_KEY = 'magnets'


class DHT(asyncio.DatagramProtocol):
    def __init__(self, port=8520, ip='0.0.0.0', max_node_qsize=1000, limit_get_peers=True, loop=None):
        self.port = port
        self.ip = ip
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.bind((ip, port))
        self.sock = sock
        self.nid = random_nid()
        self.loop = loop or asyncio.get_event_loop()
        self.transport = None
        self.redis = None
        self.running = False
        self.nodes = deque(maxlen=max_node_qsize)
        self.interval = 1 / max_node_qsize
        self.limit = limit_get_peers
        self.count = 0
        self.start_time = now()
        self.fake_nid = partial(fake_nid, self.nid)

    def bootstrap(self):
        for address in TRACKERS:
            self.send_find_node(address)

    def send_find_node(self, address, nid=None):
        nid = self.fake_nid(nid) if nid else self.nid
        tid = random_tid()
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

    def send_error(self, tid, address, code=202, msg='Server Error'):
        ret = dict(
            t=tid,
            y='e',
            e=[code, msg]
        )
        self.send_krpc(ret, address)

    def send_krpc(self, msg, address):
        if not self.transport:
            return
        try:
            self.transport.sendto(bencode(msg), address)
        except OSError:
            pass

    def on_message(self, msg, address):
        type = msg.get('y', b'e')
        if type == b'e':
            return
        if type == b'r':
            if 'nodes' in msg['r']:
                asyncio.ensure_future(self.on_find_node_reply(msg))
        elif type == b'q':
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
        nodes = split_nodes(msg['r']['nodes'])
        for node in nodes:
            nid, ip, port = node
            if len(nid) != 20 or ip == self.ip or port < 1 or port > 65536:
                continue
            n = Node(nid, ip, port)
            self.nodes.append(n)
            await asyncio.sleep(self.interval)

    def on_get_peers(self, msg, address):
        infohash = msg['a']['info_hash']
        tid = msg['t']
        nid = msg['a']['id']
        try:
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
        except KeyError:
            self.send_error(tid, address, 203, 'No HashInfo')
        if not self.limit:
            self.on_get_infohash(infohash)

    def on_announce_peer(self, msg, address):
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

    def on_get_infohash(self, infohash):
        asyncio.ensure_future(self.handle(encode_infohash(infohash)), loop=self.loop)

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, address):
        try:
            msg = bdecode(data)
            self.on_message(msg, address)
        except BTFailure:
            pass

    async def wait_reply(self):
        while len(self.nodes) == 0:
            await asyncio.sleep(self.interval)

    async def auto_find_node(self):
        print('running...')
        self.bootstrap()
        self.running = True
        await self.wait_reply()
        while self.running:
            try:
                node = self.nodes.popleft()
                self.send_find_node((node.ip, node.port), node.nid)
                await asyncio.sleep(self.interval)
            except IndexError:
                self.bootstrap()
                await self.wait_reply()

    async def connect_redis(self):
        self.redis = await aioredis.create_redis_pool('redis://localhost', minsize=10, maxsize=100, loop=self.loop)
        print('redis done')

    async def stop_redis(self):
        self.redis.close()
        await self.redis.wait_closed()

    async def handle(self, infohash):
        self.count += 1
        print(f'收集到{self.count}个infohash', end='\r')
        await self.redis.sadd(REDIS_KEY, infohash)

    def start_server(self):
        listen = self.loop.create_datagram_endpoint(lambda: self, sock=self.sock)
        task_listen = asyncio.ensure_future(listen,loop=self.loop)
        self.loop.run_until_complete(asyncio.gather(task_listen, self.connect_redis()))
        self.transport, _ = task_listen.result()
        asyncio.ensure_future(self.auto_find_node(), loop=self.loop)
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.running = False
            self.loop.run_until_complete(asyncio.gather(self.stop_redis(),self.loop.shutdown_asyncgens()))
        finally:
            self.transport.close()
            self.loop.stop()
            print(f'收集到{self.count}个infohash')


if __name__ == '__main__':
    dht = DHT(port=8426)
    dht.start_server()
