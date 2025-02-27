import asyncio

from fluid.utils.worker import Worker


class SimpleWorker(Worker):
    async def run(self):
        while self.is_running():
            self.print_message()
            await asyncio.sleep(1)

    def print_message(self):
        print(f"Hello from {self.worker_name} in state {self.worker_state}")



async def main():
    worker = SimpleWorker()
    worker.print_message()
    await worker.startup()
    asyncio.get_event_loop().call_later(5, worker.gracefully_stop)
    await worker.wait_for_shutdown()
    worker.print_message()


if __name__ == "__main__":
    asyncio.run(main())
