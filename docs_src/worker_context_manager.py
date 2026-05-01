import asyncio

import anthropic

from fluid.utils.worker import QueueConsumer

ARTICLES = [
    (
        "Async programming in Python allows multiple tasks to run concurrently "
        "within a single thread by yielding control at await points, which is "
        "particularly efficient for I/O-bound workloads such as HTTP requests "
        "and database queries."
    ),
    (
        "Large language models are trained on vast corpora of text using "
        "self-supervised objectives, enabling them to learn grammar, facts, and "
        "reasoning patterns that can be adapted to a wide range of downstream "
        "tasks through prompting or fine-tuning."
    ),
    (
        "The observer pattern decouples event producers from consumers by "
        "introducing an intermediary that maintains a list of subscribers and "
        "notifies them when state changes, making it straightforward to add new "
        "listeners without modifying the source."
    ),
]


class AnthropicWorker(QueueConsumer[str]):

    def __init__(self):
        super().__init__()
        self.results = []

    async def run(self) -> None:
        async with anthropic.AsyncAnthropic() as client:
            while not self.is_stopping():
                item = await self.get_message()
                if item is None:
                    continue
                if item == "":
                    break
                async with client.messages.stream(
                    model="claude-opus-4-7",
                    max_tokens=128,
                    messages=[
                        {
                            "role": "user",
                            "content": f"Summarise in one sentence: {item}",
                        }
                    ],
                ) as stream:
                    self.results.append(await stream.get_final_text())


async def main() -> None:
    async with AnthropicWorker() as worker:
        for article in ARTICLES:
            worker.send(article)
        worker.send("")  # signal end of input
    print(worker.results)


if __name__ == "__main__":
    asyncio.run(main())
