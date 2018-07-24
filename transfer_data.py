import aioredis
import asyncio
from collections import deque
from multiprocessing import cpu_count, Process


class TransferData:
    def __init__(self):
        self.complete = False
        self.host_remote = 'redis://203.195.209.101'
        self.host_local = 'redis://localhost'
        self.datas = deque(maxlen=2000)
        self.redis_remote = None
        self.redis_local = None
        self.redis_key_remote = 'magnets'
        self.redis_key_local = 'magnets'

    async def connect_redis_remote(self):
        self.redis_remote = await aioredis.create_redis_pool(self.host_remote)

    async def connect_redis_local(self):
        self.redis_local = await aioredis.create_redis_pool(self.host_local)

    async def down_data_loop(self):
        while True:
            data = await self.redis_remote.spop(self.redis_key_remote)
            if data is None:
                self.complete = True
                return
            fdata = f'magnet:?xt=urn:btih:{data.decode()}'
            self.datas.append(fdata)

    async def load_data_loop(self):
        while True:
            try:
                data = self.datas.popleft()
                await self.redis_local.sadd(self.redis_key_local, data)
            except IndexError:
                if self.complete:
                    return
                await self.empty_waiting()

    async def empty_waiting(self):
        while len(self.datas) == 0:
            await asyncio.sleep(0.1)

    def start(self):
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(asyncio.gather(self.connect_redis_local(), self.connect_redis_remote()))
            loop.run_until_complete(asyncio.gather(self.down_data_loop(), self.load_data_loop()))
        except Exception as e:
            print('err', e)

    def start_mulit(self):
        cpus = cpu_count()
        ps = []
        try:
            for i in range(cpus):
                process = Process(target=self.start, name=str(i))
                ps.append(process)
                process.start()
                print('process', process.name, 'start')
            for p in ps:
                p.join()
        except KeyboardInterrupt:
            for p in ps:
                p.terminate()


if __name__ == '__main__':
    t = TransferData()
    t.start_mulit()
